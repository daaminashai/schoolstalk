// Lightweight Slack notifier for posting a parent message per run
// and streaming log lines into its thread. Enabled only when env vars exist.

type SlackPostResp = {
    ok: boolean;
    error?: string;
    ts?: string;
};

export interface SlackNotifierOptions {
    token: string; // xoxb-*
    channel: string; // C...
    alertUserId?: string; // U... to @-mention on error
}

export class SlackNotifier {
    private token: string;
    private channel: string;
    private alertUserId?: string;
    private lastError?: string;

    constructor(opts: SlackNotifierOptions) {
        this.token = opts.token;
        this.channel = opts.channel;
        this.alertUserId = opts.alertUserId?.trim() || undefined;
    }

    /** Post a top-level message and return its ts for threading */
    async startThread(text: string): Promise<string | null> {
        const r = await this.postMessage({ text });
        if (!r.ok || !r.ts) return null;
        return r.ts;
    }

    /** Post a message in an existing thread */
    async postInThread(threadTs: string, text: string): Promise<void> {
        await this.postMessage({ text, thread_ts: threadTs });
    }

    /** Post an error message, pinging alert user if configured */
    async postError(threadTs: string | null, text: string): Promise<void> {
        const mention = this.alertUserId ? `<@${this.alertUserId}> ` : "";
        if (threadTs) {
            await this.postMessage({ text: `${mention}❌ ${text}`, thread_ts: threadTs });
        } else {
            await this.postMessage({ text: `${mention}❌ ${text}` });
        }
    }

    private async postMessage(payload: { text: string; thread_ts?: string }): Promise<SlackPostResp> {
        try {
            const res = await fetch("https://slack.com/api/chat.postMessage", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json; charset=utf-8",
                    Authorization: `Bearer ${this.token}`,
                },
                body: JSON.stringify({
                    channel: this.channel,
                    text: payload.text.slice(0, 39000), // Slack limit ~40k
                    thread_ts: payload.thread_ts,
                    unfurl_links: false,
                    unfurl_media: false,
                }),
            });
            const data = (await res.json()) as SlackPostResp;
            if (!data.ok) this.lastError = data.error || this.lastError || "unknown_error";
            return data;
        } catch {
            // swallow network errors to avoid crashing the scraper
            this.lastError = "network_error";
            return { ok: false, error: "network_error" };
        }
    }

    getLastError(): string | undefined {
        return this.lastError;
    }
}

export function createSlackFromEnv(): SlackNotifier | null {
    const token = slackEnvValue("SLACK_BOT_TOKEN");
    const channel = slackEnvValue("SLACK_CHANNEL_ID");
    if (!token || !channel) return null;
    const alertUserId = slackEnvValue("SLACK_ALERT_USER") ?? undefined;
    return new SlackNotifier({ token, channel, alertUserId });
}

function slackEnvValue(name: string): string | null {
    const value = process.env[name]?.trim();
    if (!value || /^(something|changeme|todo|none|null)$/i.test(value)) return null;
    return value;
}

// Aggregates per-school logs and posts a single concise Slack message
export class SlackThreadBuffer {
    constructor(private slack: SlackNotifier, private threadTs: string | null) {}

    private phases: string[] = [];
    private warnings: string[] = [];
    private infos: string[] = [];
    private liveUrl: string | null = null;
    private retried = false;

    addPhase(label: string): void {
        if (!label) return;
        const compact = label.toLowerCase();
        if (this.phases.at(-1) !== compact) this.phases.push(compact);
    }

    addMilestone(msg: string, level?: string | null): void {
        if (!msg) return;
        const trimmed = msg.trim();
        if (!trimmed) return;
        if (level === "warn") this.warnings.push(trimmed);
        else this.infos.push(trimmed);
        // Cap stored entries to keep memory and final message short
        if (this.warnings.length > 5) this.warnings = this.warnings.slice(-5);
        if (this.infos.length > 5) this.infos = this.infos.slice(-5);
    }

    addLive(url: string): void {
        if (url) this.liveUrl = url;
    }

    markRetried(): void {
        this.retried = true;
    }

    async finalizeSuccess(teachers: number, durationSec: string, csvPath: string): Promise<void> {
        if (!this.threadTs) return;
        const lines: string[] = [];
        lines.push(`✅ Done`);
        lines.push(`• Teachers: *${teachers}*`);
        lines.push(`• Duration: ${durationSec}s`);
        lines.push(`• CSV: \`${csvPath}\``);
        if (this.liveUrl) lines.push(`• Live: ${this.liveUrl}`);
        if (this.retried) lines.push(`• Retried: once`);
        if (this.warnings.length > 0) {
            const warns = this.warnings.map((w) => `  • ${w}`).join("\n");
            lines.push(`• Warnings:\n${warns}`);
        }
        if (this.phases.length > 0) {
            const chain = this.phases.map((p) => p.replace(/\s+/g, ' ')).join(' → ');
            lines.push(`• Phases: ${chain}`);
        }
        const text = lines.join("\n");
        await this.slack.postInThread(this.threadTs, text);
    }

    async finalizeFailure(errorMsg: string): Promise<void> {
        const mentionPrefix = ""; // SlackNotifier.postError will add mention if configured
        const lines: string[] = [];
        lines.push(`${mentionPrefix}❌ Failed`);
        lines.push(`• Error: ${errorMsg}`);
        if (this.phases.length > 0) lines.push(`• Last phase: ${this.phases.at(-1)}`);
        if (this.liveUrl) lines.push(`• Live: ${this.liveUrl}`);
        if (this.retried) lines.push(`• Retried: once`);
        if (!this.threadTs) {
            // Fall back to top-level post if no thread
            await this.slack.postError(null, lines.join("\n"));
        } else {
            await this.slack.postError(this.threadTs, lines.join("\n"));
        }
    }
}
