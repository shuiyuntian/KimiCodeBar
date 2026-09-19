import Foundation

// MARK: - 本地服务客户端（REST + 实例发现）

/// Kimi Code 本地服务实例（桌面版 / kimi web 共用同一套 REST 与 WS 协议）
struct KimiServerEndpoint {
    let host: String
    let port: Int

    var baseURL: String { "http://\(host):\(port)" }
    var wsURL: String { "ws://\(host):\(port)/api/v1/ws" }
}

/// 会话内的 agent 描述（main / sub / independent）
struct KimiAgentInfo {
    let agentId: String
    /// main | sub | independent（未知时按 main 处理）
    let type: String
    let label: String?
}

/// 已完成 step 的精确速度与首 Token 延迟（服务端口径）
struct KimiCompletedStep {
    let output: Int
    let streamMs: Int
    let ttftMs: Int

    /// 精确输出速度（tokens/s），无有效耗时时为 nil
    var tokensPerSec: Double? {
        guard streamMs > 0, output > 0 else { return nil }
        return Double(output) / (Double(streamMs) / 1000)
    }
}

enum KimiServerClient {
    /// KIMI_CODE_HOME 根目录（与 KimiLocalUsageService 同口径）
    static func kimiCodeHome() -> String {
        ProcessInfo.processInfo.environment["KIMI_CODE_HOME"]
            ?? (NSHomeDirectory() + "/.kimi-code")
    }

    /// 发现当前活跃的本地服务实例：读 server/instances/*.json，取心跳最新且 60s 内的实例
    static func discoverServer() -> KimiServerEndpoint? {
        let dir = kimiCodeHome() + "/server/instances"
        guard let names = try? FileManager.default.contentsOfDirectory(atPath: dir) else { return nil }
        let nowMs = Date().timeIntervalSince1970 * 1000
        var best: KimiServerEndpoint?
        var bestHeartbeat: Double = 0
        for name in names where name.hasSuffix(".json") {
            guard let data = FileManager.default.contents(atPath: dir + "/" + name),
                  let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let host = obj["host"] as? String,
                  let port = obj["port"] as? Int,
                  let heartbeat = obj["heartbeat_at"] as? Double,
                  nowMs - heartbeat < 60_000,
                  heartbeat > bestHeartbeat else { continue }
            best = KimiServerEndpoint(host: host, port: port)
            bestHeartbeat = heartbeat
        }
        return best
    }

    /// 本地服务认证 token（server.token，Bearer；不存在则 nil）
    static func bearerToken() -> String? {
        guard let data = FileManager.default.contents(atPath: kimiCodeHome() + "/server.token"),
              let token = String(data: data, encoding: .utf8)?
                  .trimmingCharacters(in: .whitespacesAndNewlines),
              !token.isEmpty else { return nil }
        return token
    }

    // MARK: REST

    /// 活跃会话列表（busy 或 main_turn_active 的会话 id），返回会话 id 数组
    static func fetchActiveSessions(pageSize: Int = 50) async -> [String] {
        guard let obj = await getJSON("/api/v1/sessions?page_size=\(pageSize)"),
              let data = obj["data"] as? [String: Any],
              let items = data["items"] as? [[String: Any]] else { return [] }
        return items.compactMap { item in
            let active = (item["busy"] as? Bool) == true
                || (item["main_turn_active"] as? Bool) == true
            guard active, let id = item["id"] as? String else { return nil }
            return id
        }
    }

    /// 会话的 agent 列表（含子 agent），失败返回空数组
    static func fetchSessionAgents(sessionId: String) async -> [KimiAgentInfo] {
        guard let obj = await getJSON("/api/v1/sessions/\(sessionId)"),
              let data = obj["data"] as? [String: Any],
              let agents = data["agents"] as? [[String: Any]] else { return [] }
        return agents.compactMap { agent in
            guard let agentId = agent["agentId"] as? String ?? agent["agent_id"] as? String,
                  !agentId.isEmpty else { return nil }
            return KimiAgentInfo(
                agentId: agentId,
                type: agent["type"] as? String ?? "main",
                label: agent["label"] as? String
            )
        }
    }

    /// 取某 agent 最近一个已完成且有输出记录的 step（精确速度与 TTFT 兜底）
    static func fetchLatestCompletedStep(sessionId: String, agentId: String) async -> KimiCompletedStep? {
        guard let obj = await getJSON("/api/v1/sessions/\(sessionId)/transcript?agent_id=\(agentId)&page_size=10"),
              let data = obj["data"] as? [String: Any],
              let items = data["items"] as? [[String: Any]] else { return nil }
        for item in items.reversed() where item["kind"] as? String == "turn" {
            guard let steps = item["steps"] as? [[String: Any]] else { continue }
            for step in steps.reversed() where step["state"] as? String == "completed" {
                guard let usage = step["usage"] as? [String: Any],
                      let output = (usage["output"] as? NSNumber)?.intValue, output > 0,
                      let timing = step["timing"] as? [String: Any],
                      let streamMs = (timing["llmStreamDurationMs"] as? NSNumber)?.intValue, streamMs > 0 else { continue }
                return KimiCompletedStep(
                    output: output,
                    streamMs: streamMs,
                    ttftMs: (timing["llmFirstTokenLatencyMs"] as? NSNumber)?.intValue ?? 0
                )
            }
        }
        return nil
    }

    // MARK: 底层请求

    /// GET JSON（统一信封 { code, msg, data }，code == 0 才返回），失败静默返回 nil
    private static func getJSON(_ path: String) async -> [String: Any]? {
        guard let endpoint = discoverServer(),
              let url = URL(string: endpoint.baseURL + path) else { return nil }
        var request = URLRequest(url: url)
        request.timeoutInterval = 3
        if let token = bearerToken() {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        guard let (data, _) = try? await URLSession.shared.data(for: request),
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              (obj["code"] as? NSNumber)?.intValue == 0 else { return nil }
        return obj
    }
}
