import SwiftUI

// MARK: - Token 速度卡片

/// 「Token 速度」卡片：周期统计（平均输出速度 + 平均首Token延迟，来源 wire.jsonl 增量扫描）
/// + 实时区（WebSocket delta 流滑动窗口估算，含子 agent 分行）。
/// 周期三档：累计 / 今日 / 7天（独立持久化 tokenSpeedRange）。
struct TokenSpeedCard: View {
    @StateObject private var usageService = KimiLocalUsageService.shared
    @StateObject private var liveService = KimiLiveSpeedService.shared
    @StateObject private var languageManager = LanguageManager.shared
    @AppStorage("tokenSpeedRange") private var rangeRaw: String = LocalUsageRange.all.rawValue
    @State private var hoveredSegment: LocalUsageRange?
    @State private var pulse = false

    private var range: LocalUsageRange {
        LocalUsageRange(rawValue: rangeRaw) ?? .all
    }

    /// 周期统计：随范围联动（首次扫描未完成前为 nil，走骨架屏）
    private var stats: (tokensPerSec: Double?, ttftMs: Double?)? {
        usageService.hasScanned ? usageService.speedStats(in: range) : nil
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            headerRow

            if let stats {
                metricsRow(speed: stats.tokensPerSec, ttftMs: stats.ttftMs)
            } else {
                metricsSkeleton
            }

            liveSection
        }
        .padding(14)
        .background(Color.kimiCardBackground)
        .clipShape(RoundedRectangle(cornerRadius: 14))
    }

    // MARK: 标题 + 范围切换

    private var headerRow: some View {
        HStack {
            LText("Token 速度")
                .font(.system(size: 13, weight: .medium))
                .foregroundStyle(.kimiTextPrimary)

            Spacer()

            rangePicker
        }
    }

    private var rangePicker: some View {
        HStack(spacing: 2) {
            ForEach(LocalUsageRange.allCases) { item in
                let isSelected = range == item
                let isHovered = hoveredSegment == item
                Text(item.displayName)
                    .font(.system(size: 11, weight: .medium))
                    .foregroundStyle(isSelected ? Color.white : (isHovered ? Color.kimiTextPrimary : Color.kimiTextSecondary))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 3)
                    .background(
                        Capsule()
                            .fill(isSelected ? Color.kimiBlue : Color.kimiTextPrimary.opacity(isHovered ? 0.08 : 0))
                    )
                    .contentShape(Capsule())
                    .onTapGesture { rangeRaw = item.rawValue }
                    .onHover { hoveredSegment = $0 ? item : nil }
                    .cursor(.pointingHand)
            }
        }
        .padding(2)
        .background(Color.kimiTextPrimary.opacity(0.06))
        .clipShape(Capsule())
    }

    // MARK: 周期统计行（平均输出速度 + 平均首Token延迟）

    private func metricsRow(speed: Double?, ttftMs: Double?) -> some View {
        HStack(alignment: .top) {
            VStack(alignment: .leading, spacing: 2) {
                HStack(alignment: .lastTextBaseline, spacing: 3) {
                    Text(Self.formatSpeedValue(speed))
                        .font(.system(size: 18, weight: .bold))
                        .foregroundStyle(.kimiTextPrimary)
                    Text("tokens/s")
                        .font(.system(size: 11, weight: .medium))
                        .foregroundStyle(.kimiTextSecondary)
                }

                LText("平均输出速度")
                    .font(.system(size: 11))
                    .foregroundStyle(.kimiTextTertiary)
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 2) {
                Text(Self.formatTTFT(ttftMs))
                    .font(.system(size: 18, weight: .bold))
                    .foregroundStyle(.kimiTextPrimary)

                LText("平均首Token延迟")
                    .font(.system(size: 11))
                    .foregroundStyle(.kimiTextTertiary)
            }
        }
    }

    private var metricsSkeleton: some View {
        HStack(alignment: .top) {
            VStack(alignment: .leading, spacing: 6) {
                skeletonBlock(width: 76, height: 22)
                skeletonBlock(width: 60, height: 12)
            }
            Spacer()
            VStack(alignment: .trailing, spacing: 6) {
                skeletonBlock(width: 56, height: 22)
                skeletonBlock(width: 76, height: 12)
            }
        }
    }

    private func skeletonBlock(width: CGFloat, height: CGFloat) -> some View {
        RoundedRectangle(cornerRadius: 4)
            .fill(Color.kimiTextPrimary.opacity(0.10))
            .frame(width: width, height: height)
    }

    // MARK: 实时区

    @ViewBuilder
    private var liveSection: some View {
        switch liveService.state {
        case .live:
            liveRows
        case .idle, .connecting:
            LText("空闲")
                .font(.system(size: 11))
                .foregroundStyle(.kimiTextTertiary)
        case .unreachable:
            LText("未检测到本地服务")
                .font(.system(size: 11))
                .foregroundStyle(.kimiTextTertiary)
        }
    }

    /// 实时合计行 + 按 agent 分行（主会话在前，子代理随后）
    private var liveRows: some View {
        VStack(alignment: .leading, spacing: 6) {
            Divider()

            HStack(spacing: 6) {
                Circle()
                    .fill(Color.green)
                    .frame(width: 6, height: 6)
                    .scaleEffect(pulse ? 1.3 : 1)
                    .opacity(pulse ? 0.5 : 1)
                    .onAppear {
                        withAnimation(.easeInOut(duration: 0.9).repeatForever(autoreverses: true)) {
                            pulse = true
                        }
                    }

                LText("生成中")
                    .font(.system(size: 11, weight: .medium))
                    .foregroundStyle(.kimiTextSecondary)

                Spacer()

                if let total = liveService.totalLiveTokensPerSec {
                    Text("~\(Self.formatSpeedValue(total)) tokens/s")
                        .font(.system(size: 11, weight: .semibold, design: .monospaced))
                        .foregroundStyle(.kimiTextPrimary)
                }
            }

            ForEach(Array(liveService.agentSpeeds.enumerated()), id: \.element.id) { index, agent in
                agentRow(agent, index: index)
            }
        }
    }

    private func agentRow(_ agent: KimiAgentSpeed, index: Int) -> some View {
        HStack(spacing: 6) {
            Circle()
                .fill(agent.liveTokensPerSec != nil ? Color.kimiBlue : Color.kimiTextTertiary.opacity(0.4))
                .frame(width: 5, height: 5)

            agentLabel(agent, index: index)
                .font(.system(size: 11))
                .foregroundStyle(.kimiTextSecondary)
                .lineLimit(1)

            if let inFlight = agent.inFlightSince {
                Text(String(format: languageManager.tr("已生成 %1$@"), Self.formatElapsed(inFlight)))
                    .font(.system(size: 10))
                    .foregroundStyle(.kimiTextTertiary)
            }

            Spacer()

            if let live = agent.liveTokensPerSec {
                // ~ 前缀表达估算语义（服务端 mid-step 不下发 token 数）
                Text("~\(Self.formatSpeedValue(live))")
                    .font(.system(size: 11, weight: .medium, design: .monospaced))
                    .foregroundStyle(.kimiTextPrimary)
            } else if let last = agent.lastStepSpeed {
                Text(Self.formatSpeedValue(last))
                    .font(.system(size: 11, weight: .medium, design: .monospaced))
                    .foregroundStyle(.kimiTextTertiary)
            }
        }
    }

    /// 主会话固定文案；子代理用服务端 label，缺失时回退「子代理 N」
    private func agentLabel(_ agent: KimiAgentSpeed, index: Int) -> Text {
        if !agent.isSub {
            return Text(languageManager.tr("主会话"))
        }
        if !agent.label.isEmpty {
            return Text(agent.label)
        }
        return Text(String(format: languageManager.tr("子代理 %1$@"), "\(index)"))
    }

    // MARK: 格式化

    /// 速度：≥100 取整，否则一位小数；nil 显示 --
    static func formatSpeedValue(_ value: Double?) -> String {
        guard let value else { return "--" }
        return value >= 100 ? String(format: "%.0f", value) : String(format: "%.1f", value)
    }

    /// 首 Token 延迟：<1s 显示 ms，≥1s 显示秒
    static func formatTTFT(_ ms: Double?) -> String {
        guard let ms else { return "--" }
        if ms < 1000 { return String(format: "%.0f ms", ms) }
        return String(format: "%.1f s", ms / 1000)
    }

    /// 已生成时长：<60s 显示秒，否则分+秒
    static func formatElapsed(_ since: Date) -> String {
        let seconds = max(Int(Date().timeIntervalSince(since)), 0)
        if seconds < 60 { return "\(seconds)s" }
        return String(format: "%d:%02d", seconds / 60, seconds % 60)
    }
}
