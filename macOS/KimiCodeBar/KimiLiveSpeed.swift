import Foundation

// MARK: - Token 速度实时数据模型

/// 单个 agent 的实时速度展示模型
struct KimiAgentSpeed: Identifiable {
    var id: String { "\(sessionId)|\(agentId)" }
    let agentId: String
    let sessionId: String
    /// agent 名称（子代理用服务端 label，可能为空）
    let label: String
    /// 是否子 agent
    let isSub: Bool
    /// 滑动窗口估算的实时输出速度（tokens/s）；nil = 当前未在生成
    let liveTokensPerSec: Double?
    /// 最近完成 step 的服务端精确速度（tokens/s）
    let lastStepSpeed: Double?
    /// 最近完成 step 的首 Token 延迟（ms）
    let lastStepTTFTMs: Double?
    /// 进行中 step 的开始时间（用于「已生成 xx s」）
    let inFlightSince: Date?
}

enum KimiLiveSpeedState: Equatable {
    /// 服务可达但当前无进行中的生成
    case idle
    /// 连接 / 重连中
    case connecting
    /// 有 agent 正在生成
    case live
    /// 未发现本地服务
    case unreachable
}

/// 引擎 → 主线程服务的快照
struct KimiLiveSpeedSnapshot {
    var state: KimiLiveSpeedState
    var agents: [KimiAgentSpeed]
    var totalLive: Double?
    /// 最近 60 秒的合计速度时间线（每秒一个值，旧 → 新），用于曲线绘制
    var speedTimeline: [Double]
}

// MARK: - 实时速度服务

/// Token 速度实时服务：WebSocket 订阅本地服务 delta 流，按 agent 估算实时输出速度。
/// 生命周期跟随面板可见性：start() / stop() 幂等。
/// 实时速度为估算值（服务端 mid-step 不下发 token 数）：CJK 字符 ≈ 1/1.5 token，
/// 非 CJK 字符 ≈ 1/4 token；step 完成后以服务端精确值（output/streamMs）为准。
@MainActor
final class KimiLiveSpeedService: ObservableObject {
    static let shared = KimiLiveSpeedService()

    @Published private(set) var state: KimiLiveSpeedState = .idle
    /// 活跃 agent 速度列表（主会话在前，子代理按 label 排序）
    @Published private(set) var agentSpeeds: [KimiAgentSpeed] = []
    /// 全部活跃 agent 的实时速度合计
    @Published private(set) var totalLiveTokensPerSec: Double? = nil
    /// 最近 60 秒合计速度时间线（每秒一个值，旧 → 新），仅面板打开期间采集
    @Published private(set) var speedTimeline: [Double] = []

    private var engine: Engine?

    private init() {}

    /// 启动实时连接（幂等）。引擎常驻运行：菜单栏实时指标在面板关闭时也需要数据。
    func start() {
        guard engine == nil else { return }
        let engine = Engine()
        self.engine = engine
        engine.start { [weak self] snapshot in
            Task { @MainActor in self?.apply(snapshot) }
        }
    }

    /// 停止实时连接（目前仅退出 App 时调用）
    func stop() {
        engine?.stop()
        engine = nil
        state = .idle
        agentSpeeds = []
        totalLiveTokensPerSec = nil
        speedTimeline = []
    }

    private func apply(_ snapshot: KimiLiveSpeedSnapshot) {
        state = snapshot.state
        agentSpeeds = snapshot.agents
        totalLiveTokensPerSec = snapshot.totalLive
        speedTimeline = snapshot.speedTimeline
    }
}

// MARK: - WebSocket 引擎

private extension KimiLiveSpeedService {
    final class Engine: @unchecked Sendable {
        /// 实时速度滑动窗口时长
        private let windowSeconds: TimeInterval = 8
        /// 样本超过该间隔未更新即视为不在生成
        private let staleSeconds: TimeInterval = 5
        /// 无活动多久后移除 tracker
        private let pruneSeconds: TimeInterval = 60

        private let queue = DispatchQueue(label: "com.kimicodebar.live-speed")
        private var onUpdate: (@Sendable (KimiLiveSpeedSnapshot) -> Void)?

        private var urlSession: URLSession?
        private var socket: URLSessionWebSocketTask?
        private var stopped = false
        private var reconnectAttempt = 0
        private var reconnectWork: DispatchWorkItem?
        private var watchdog: DispatchSourceTimer?
        private var reconcile: DispatchSourceTimer?
        private var heartbeatMs: Double = 10_000
        private var lastFrameAt = Date.distantPast
        private var lastPublishAt = Date.distantPast
        private var lastPublishedState: KimiLiveSpeedState?

        private struct Tracker {
            var sessionId: String
            var label: String
            var isSub: Bool
            var samples: [(date: Date, tokens: Double)] = []
            var inFlightSince: Date?
            var lastStepSpeed: Double?
            var lastStepTTFTMs: Double?
            var lastActivity: Date = .distantPast
        }

        /// key = sessionId|agentId
        private var trackers: [String: Tracker] = [:]
        private var subscribed: Set<String> = []
        private var subscribedSessions: Set<String> = []

        /// 60 秒速度时间线环形缓冲（按下标 = 秒 % 60 归桶，timelineSeconds 校验有效期）
        private var timelineTokens = [Double](repeating: 0, count: 60)
        private var timelineSeconds = [Int](repeating: -1, count: 60)

        func start(_ onUpdate: @escaping @Sendable (KimiLiveSpeedSnapshot) -> Void) {
            self.onUpdate = onUpdate
            queue.async { self.connect() }
        }

        func stop() {
            queue.async {
                self.stopped = true
                self.reconnectWork?.cancel()
                self.watchdog?.cancel()
                self.reconcile?.cancel()
                self.teardownSocket()
                self.trackers.removeAll()
                self.subscribed.removeAll()
                self.subscribedSessions.removeAll()
            }
        }

        // MARK: 连接管理

        private func connect() {
            guard !stopped else { return }
            guard let endpoint = KimiServerClient.discoverServer() else {
                publish(.unreachable)
                scheduleReconnect()
                return
            }

            publish(.connecting)

            var request = URLRequest(url: URL(string: endpoint.wsURL)!)
            request.timeoutInterval = 5
            if let token = KimiServerClient.bearerToken() {
                request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
            }

            let session = URLSession(configuration: .default)
            urlSession = session
            let socket = session.webSocketTask(with: request)
            self.socket = socket
            socket.resume()
            receiveLoop(socket)
            startWatchdog()

            // 握手：client_hello 后由 server_hello 触发订阅引导
            send(["type": "client_hello",
                  "id": UUID().uuidString,
                  "payload": ["client_id": "kimicodebar-\(UUID().uuidString)"]])
        }

        private func teardownSocket() {
            socket?.cancel()
            socket = nil
            urlSession?.invalidateAndCancel()
            urlSession = nil
        }

        private func handleDisconnect() {
            guard !stopped, socket != nil else { return }
            teardownSocket()
            publish(.connecting)
            scheduleReconnect()
        }

        private func scheduleReconnect() {
            let delay = min(30.0, pow(2.0, Double(reconnectAttempt)))
            reconnectAttempt += 1
            let work = DispatchWorkItem { [weak self] in
                guard let self, !self.stopped else { return }
                self.teardownSocket()
                self.connect()
            }
            reconnectWork?.cancel()
            reconnectWork = work
            queue.asyncAfter(deadline: .now() + delay, execute: work)
        }

        // MARK: 收发

        private func receiveLoop(_ socket: URLSessionWebSocketTask) {
            socket.receive { [weak self] result in
                guard let self else { return }
                switch result {
                case .failure:
                    self.queue.async { self.handleDisconnect() }
                case .success(let message):
                    self.queue.async {
                        // 重连后旧 socket 的迟到消息直接丢弃
                        guard self.socket === socket, !self.stopped else { return }
                        self.lastFrameAt = Date()
                        if case .string(let text) = message {
                            self.handleMessage(text)
                        }
                        self.receiveLoop(socket)
                    }
                }
            }
        }

        private func send(_ obj: [String: Any]) {
            guard let data = try? JSONSerialization.data(withJSONObject: obj),
                  let text = String(data: data, encoding: .utf8) else { return }
            socket?.send(.string(text)) { _ in }
        }

        private func handleMessage(_ text: String) {
            guard let data = text.data(using: .utf8),
                  let msg = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let type = msg["type"] as? String else { return }

            switch type {
            case "server_hello":
                if let payload = msg["payload"] as? [String: Any],
                   let heartbeat = (payload["heartbeat_ms"] as? NSNumber)?.doubleValue {
                    heartbeatMs = heartbeat
                }
                reconnectAttempt = 0
                // 清掉旧订阅状态，全量重建
                subscribed.removeAll()
                subscribedSessions.removeAll()
                Task { await self.bootstrapSubscriptions() }
            case "ping":
                send(["type": "pong"])
            case "transcript.ops":
                // delta 级转录流：增量操作数组（2.0.x 实测帧类型，asyncapi 未完整列出）
                if let payload = msg["payload"] as? [String: Any] {
                    handleTranscriptOps(payload)
                }
            case "session_event":
                // 会话级事件（子代理生命周期等；当前版本可能不下发，保留兼容）
                if let payload = msg["payload"] as? [String: Any] {
                    handleSessionEvent(payload, sessionId: msg["session_id"] as? String)
                }
            case "transcript.reset":
                // 转录快照重置：无需处理，后续 ops 会继续到达
                break
            case "resync_required", "error":
                queue.async { self.handleDisconnect() }
            default:
                break
            }
        }

        /// 处理 transcript.ops：append（流式文本增量）与 step.upsert（step 状态/精确数据）
        private func handleTranscriptOps(_ payload: [String: Any]) {
            let now = Date()
            let agentId = payload["agent_id"] as? String ?? "main"
            guard let sessionId = payload["session_id"] as? String,
                  let ops = payload["ops"] as? [[String: Any]] else { return }
            var hasDelta = false

            for op in ops {
                guard let opType = op["op"] as? String else { continue }
                switch opType {
                case "append":
                    // 流式文本增量：thinking / 正文 / 工具参数都经此下发
                    guard let text = op["text"] as? String, !text.isEmpty else { continue }
                    recordDelta("\(sessionId)|\(agentId)", text: text, at: now)
                    hasDelta = true
                case "step.upsert":
                    guard let step = op["step"] as? [String: Any] else { continue }
                    handleStepUpsert(sessionId: sessionId, agentId: agentId, step: step, at: now)
                default:
                    break
                }
            }
            if hasDelta { publishThrottled() }
        }

        /// step.upsert：running 记录进行中状态；completed 提取服务端精确速度与首 Token 延迟
        private func handleStepUpsert(sessionId: String, agentId: String, step: [String: Any], at now: Date) {
            let key = "\(sessionId)|\(agentId)"
            let state = step["state"] as? String

            if state == "running" {
                if trackers[key] == nil {
                    trackers[key] = Tracker(sessionId: sessionId, label: "", isSub: agentId != "main")
                }
                if let startedAt = step["startedAt"] as? String,
                   let started = Self.isoDate(startedAt) {
                    trackers[key]?.inFlightSince = started
                } else {
                    trackers[key]?.inFlightSince = now
                }
                trackers[key]?.lastActivity = now
                publishThrottled()
                return
            }

            guard state == "completed" else { return }
            if var tracker = trackers[key] {
                tracker.inFlightSince = nil
                tracker.samples.removeAll()
                tracker.lastActivity = now
                let output = (step["usage"] as? [String: Any]).flatMap { ($0["output"] as? NSNumber)?.intValue } ?? 0
                let timing = step["timing"] as? [String: Any]
                let streamMs = (timing?["llmStreamDurationMs"] as? NSNumber)?.intValue ?? 0
                if output > 0, streamMs > 0 {
                    tracker.lastStepSpeed = Double(output) / (Double(streamMs) / 1000)
                    tracker.lastStepTTFTMs = (timing?["llmFirstTokenLatencyMs"] as? NSNumber).map { Double($0.intValue) }
                } else {
                    // 无 usage/timing 时 REST 兜底
                    Task {
                        if let step = await KimiServerClient.fetchLatestCompletedStep(
                            sessionId: sessionId, agentId: agentId
                        ) {
                            self.queue.async {
                                self.trackers[key]?.lastStepSpeed = step.tokensPerSec
                                self.trackers[key]?.lastStepTTFTMs = Double(step.ttftMs)
                            }
                        }
                    }
                }
                trackers[key] = tracker
                publishThrottled()
            }
        }

        private static let isoFormatter: ISO8601DateFormatter = {
            let f = ISO8601DateFormatter()
            f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            return f
        }()

        private static func isoDate(_ string: String) -> Date? {
            isoFormatter.date(from: string) ?? ISO8601DateFormatter().date(from: string)
        }

        // MARK: 事件处理

        private func handleSessionEvent(_ payload: [String: Any], sessionId: String?) {
            guard let eventType = payload["type"] as? String else { return }
            let now = Date()

            switch eventType {
            case "assistant.delta", "thinking.delta":
                // thinking 也是模型输出 token，计入速度口径
                guard let text = payload["delta"] as? String, !text.isEmpty else { return }
                recordDelta(agentKey(payload, sessionId: sessionId), text: text, at: now)
            case "tool.call.delta":
                // 工具调用参数生成同样消耗输出 token
                guard let text = payload["argumentsPart"] as? String, !text.isEmpty else { return }
                recordDelta(agentKey(payload, sessionId: sessionId), text: text, at: now)
            case "turn.step.started":
                let key = agentKey(payload, sessionId: sessionId)
                trackers[key]?.inFlightSince = now
                trackers[key]?.lastActivity = now
                publishThrottled()
            case "turn.step.completed":
                let key = agentKey(payload, sessionId: sessionId)
                if var tracker = trackers[key] {
                    tracker.inFlightSince = nil
                    tracker.samples.removeAll()
                    tracker.lastActivity = now
                    if let step = parseCompletedStep(payload) {
                        tracker.lastStepSpeed = step.tokensPerSec
                        tracker.lastStepTTFTMs = step.ttftMs
                    } else if let sessionId {
                        // 事件不带 usage/timing 时 REST 兜底
                        let agentId = key.components(separatedBy: "|").last ?? "main"
                        Task {
                            if let step = await KimiServerClient.fetchLatestCompletedStep(
                                sessionId: sessionId, agentId: agentId
                            ) {
                                self.queue.async {
                                    self.trackers[key]?.lastStepSpeed = step.tokensPerSec
                                    self.trackers[key]?.lastStepTTFTMs = Double(step.ttftMs)
                                }
                            }
                        }
                    }
                    trackers[key] = tracker
                    publishThrottled()
                }
            case "subagent.spawned", "agent.created":
                guard let sessionId else { return }
                Task { await self.addSession(sessionId, refetch: true) }
            case "event.session.created":
                guard let sessionId else { return }
                Task { await self.addSession(sessionId) }
            case "subagent.completed", "agent.disposed":
                let key = agentKey(payload, sessionId: sessionId)
                trackers[key]?.inFlightSince = nil
                trackers[key]?.lastActivity = now
                publishThrottled()
            case "turn.ended", "prompt.completed", "prompt.aborted":
                // 会话级结束：清该会话所有 agent 的进行中状态
                guard let sessionId else { return }
                for key in trackers.keys where key.hasPrefix("\(sessionId)|") {
                    trackers[key]?.inFlightSince = nil
                    trackers[key]?.lastActivity = now
                }
                publishThrottled()
            default:
                break
            }
        }

        private func agentKey(_ payload: [String: Any], sessionId: String?) -> String {
            let agentId = payload["agentId"] as? String ?? "main"
            return "\(sessionId ?? "")|\(agentId)"
        }

        private func recordDelta(_ key: String, text: String, at date: Date) {
            let tokens = Self.estimateTokens(text)
            guard tokens > 0 else { return }
            if trackers[key] == nil {
                let sessionId = key.components(separatedBy: "|").first ?? ""
                trackers[key] = Tracker(
                    sessionId: sessionId,
                    label: "",
                    isSub: !key.hasSuffix("|main"),
                    lastActivity: date
                )
            }
            trackers[key]?.samples.append((date, tokens))
            if trackers[key]?.inFlightSince == nil {
                trackers[key]?.inFlightSince = date
            }
            trackers[key]?.lastActivity = date
            publishThrottled()
        }

        /// 文本 → token 数估算：CJK 字符 ≈ 1/1.5 token，其余 ≈ 1/4 token，非空至少 1
        static func estimateTokens(_ text: String) -> Double {
            var cjk = 0, other = 0
            for scalar in text.unicodeScalars {
                switch scalar.value {
                case 0x3000...0x30FF,   // CJK 标点、日文假名
                     0x3400...0x4DBF,    // CJK 扩展 A
                     0x4E00...0x9FFF,    // CJK 基本区
                     0xFF00...0xFFEF:    // 全角字符
                    cjk += 1
                default:
                    other += 1
                }
            }
            guard cjk + other > 0 else { return 0 }
            return max(1, Double(cjk) / 1.5 + Double(other) / 4)
        }

        /// 从 turn.step.completed 事件解析服务端精确数据（字段宽松解析）
        private func parseCompletedStep(_ payload: [String: Any]) -> (tokensPerSec: Double?, ttftMs: Double?)? {
            guard let usage = payload["usage"] as? [String: Any],
                  let timing = payload["timing"] as? [String: Any] else { return nil }
            let output = (usage["output"] as? NSNumber)?.intValue ?? 0
            let streamMs = (timing["llmStreamDurationMs"] as? NSNumber)?.intValue ?? 0
            guard output > 0, streamMs > 0 else { return nil }
            return (Double(output) / (Double(streamMs) / 1000),
                    (timing["llmFirstTokenLatencyMs"] as? NSNumber).map { Double($0.intValue) })
        }

        // MARK: 订阅引导

        /// 引导：订阅所有活跃会话的 main + 子 agent
        private func bootstrapSubscriptions() async {
            guard !stopped else { return }
            let sessionIds = await KimiServerClient.fetchActiveSessions()
            guard !stopped else { return }
            for sessionId in sessionIds {
                await addSession(sessionId)
            }
            startReconcileTimer()
            publishCurrent()
        }

        /// 会话级订阅：拉 agent 列表，为每个 agent 建立 tracker 并订阅 delta 流
        private func addSession(_ sessionId: String, refetch: Bool = false) async {
            guard !stopped else { return }
            if !refetch {
                guard subscribedSessions.insert(sessionId).inserted else { return }
            }
            var agents = await KimiServerClient.fetchSessionAgents(sessionId: sessionId)
            if agents.isEmpty {
                agents = [KimiAgentInfo(agentId: "main", type: "main", label: nil)]
            }
            for agent in agents {
                let key = "\(sessionId)|\(agent.agentId)"
                queue.async {
                    if var tracker = self.trackers[key] {
                        tracker.label = agent.label ?? tracker.label
                        tracker.isSub = agent.type != "main"
                        self.trackers[key] = tracker
                    } else {
                        self.trackers[key] = Tracker(
                            sessionId: sessionId,
                            label: agent.label ?? "",
                            isSub: agent.type != "main"
                        )
                    }
                    guard self.subscribed.insert(key).inserted else { return }
                    self.send(["type": "subscribe_v2",
                               "id": UUID().uuidString,
                               "payload": ["session_id": sessionId,
                                           "transcript": [agent.agentId: "delta"]]])
                    // 精确速度与 TTFT 基线（REST 兜底）
                    Task {
                        if let step = await KimiServerClient.fetchLatestCompletedStep(
                            sessionId: sessionId, agentId: agent.agentId
                        ) {
                            self.queue.async {
                                self.trackers[key]?.lastStepSpeed = step.tokensPerSec
                                self.trackers[key]?.lastStepTTFTMs = Double(step.ttftMs)
                            }
                        }
                    }
                }
            }
        }

        // MARK: 定时器

        /// 看门狗：超过 2 个心跳周期无任何帧视为断线，重连
        private func startWatchdog() {
            watchdog?.cancel()
            let timer = DispatchSource.makeTimerSource(queue: queue)
            timer.schedule(deadline: .now() + 5, repeating: 5)
            timer.setEventHandler { [weak self] in
                guard let self, !self.stopped, self.socket != nil else { return }
                let silence = Date().timeIntervalSince(self.lastFrameAt)
                if silence > max(self.heartbeatMs / 1000 * 2, 20) {
                    self.handleDisconnect()
                }
            }
            watchdog = timer
            timer.resume()
        }

        /// 对账：周期性发现新活跃会话并补订阅（防丢事件），已订阅会话重拉 agent 列表兜住子代理
        private func startReconcileTimer() {
            reconcile?.cancel()
            let timer = DispatchSource.makeTimerSource(queue: queue)
            timer.schedule(deadline: .now() + 10, repeating: 10)
            timer.setEventHandler { [weak self] in
                guard let self, !self.stopped else { return }
                Task {
                    let sessionIds = await KimiServerClient.fetchActiveSessions()
                    guard !self.stopped else { return }
                    for sessionId in sessionIds {
                        if self.subscribedSessions.contains(sessionId) {
                            await self.addSession(sessionId, refetch: true)
                        } else {
                            await self.addSession(sessionId)
                        }
                    }
                }
            }
            reconcile = timer
            timer.resume()
        }

        // MARK: 快照发布

        private func publish(_ state: KimiLiveSpeedState) {
            lastPublishedState = nil // 强制下一次发布
            publishSnapshot(state)
        }

        private func publishThrottled() {
            publishSnapshot(nil)
        }

        private func publishCurrent() {
            publishSnapshot(nil)
        }

        private func publishSnapshot(_ forcedState: KimiLiveSpeedState?) {
            let now = Date()
            // 非强制发布做 500ms 节流
            if forcedState == nil, now.timeIntervalSince(lastPublishAt) < 0.5 {
                return
            }
            let snapshot = buildSnapshot(now: now, forcedState: forcedState)
            lastPublishAt = now
            lastPublishedState = snapshot.state
            onUpdate?(snapshot)
        }

        private func buildSnapshot(now: Date, forcedState: KimiLiveSpeedState?) -> KimiLiveSpeedSnapshot {
            // 清理长期无活动的 tracker
            for key in trackers.keys {
                guard let tracker = trackers[key] else { continue }
                if tracker.inFlightSince == nil, now.timeIntervalSince(tracker.lastActivity) > pruneSeconds {
                    trackers.removeValue(forKey: key)
                    subscribed.remove(key)
                }
            }

            var agents: [KimiAgentSpeed] = []
            var total: Double = 0

            for (key, tracker) in trackers {
                let cutoff = now.addingTimeInterval(-windowSeconds)
                let recent = tracker.samples.filter { $0.date >= cutoff }
                let isActive = now.timeIntervalSince(tracker.lastActivity) <= staleSeconds

                var speed: Double? = nil
                if isActive, !recent.isEmpty {
                    let sum = recent.reduce(0) { $0 + $1.tokens }
                    let base = tracker.inFlightSince ?? cutoff
                    let denominator = min(windowSeconds, max(now.timeIntervalSince(base), 1))
                    speed = sum / denominator
                }
                if let speed { total += speed }

                // 无进行中、无近期活动、无历史精确数据的 tracker 不上屏
                guard tracker.inFlightSince != nil
                        || isActive
                        || tracker.lastStepSpeed != nil else { continue }

                agents.append(KimiAgentSpeed(
                    agentId: key.components(separatedBy: "|").last ?? "",
                    sessionId: tracker.sessionId,
                    label: tracker.label,
                    isSub: tracker.isSub,
                    liveTokensPerSec: speed,
                    lastStepSpeed: tracker.lastStepSpeed,
                    lastStepTTFTMs: tracker.lastStepTTFTMs,
                    inFlightSince: tracker.inFlightSince
                ))
            }

            agents.sort {
                if $0.isSub != $1.isSub { return !$0.isSub }
                return $0.label < $1.label
            }

            let state: KimiLiveSpeedState
            if let forcedState {
                state = forcedState
            } else if socket == nil {
                state = .connecting
            } else {
                state = agents.isEmpty ? .idle : .live
            }
            return KimiLiveSpeedSnapshot(
                state: state,
                agents: agents,
                totalLive: total > 0 ? total : nil,
                speedTimeline: buildTimeline(now: now)
            )
        }

        /// 汇总全部 agent 最近 60 秒的 delta，按秒归桶得到速度时间线（旧 → 新）
        private func buildTimeline(now: Date) -> [Double] {
            for tracker in trackers.values {
                for sample in tracker.samples where now.timeIntervalSince(sample.date) <= 60 {
                    let second = Int(sample.date.timeIntervalSince1970)
                    let index = second % 60
                    if timelineSeconds[index] == second {
                        timelineTokens[index] += sample.tokens
                    } else {
                        timelineSeconds[index] = second
                        timelineTokens[index] = sample.tokens
                    }
                }
            }
            let currentSecond = Int(now.timeIntervalSince1970)
            return (0..<60).map { offset in
                let second = currentSecond - 59 + offset
                let index = second % 60
                return timelineSeconds[index] == second ? timelineTokens[index] : 0
            }
        }
    }
}
