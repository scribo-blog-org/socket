import {
    createReadStream,
    createWriteStream,
    mkdirSync,
    readFileSync,
    readdirSync,
    renameSync,
    statSync,
    unlinkSync,
    type WriteStream,
} from 'fs';
import { join } from 'path';
import { pipeline } from 'stream';
import { createGzip } from 'zlib';

export type RuntimeLevel = 'debug' | 'info' | 'warn' | 'error';

const ORDER: Record<RuntimeLevel, number> = {
    debug: 10,
    info: 20,
    warn: 30,
    error: 40,
};

const SECRET_KEY =
    /pass(word)?|secret|token|authorization|cookie|api[_-]?key|private|signature/i;
const MAX_DEPTH = 4;
const MAX_STRING = 2000;
const PRUNE_EVERY_MS = 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;

export function packageVersion(cwd = process.cwd()): string | undefined {
    try {
        const pkg = JSON.parse(
            readFileSync(join(cwd, 'package.json'), 'utf8'),
        ) as { version?: string };
        return pkg.version || undefined;
    } catch {
        return undefined;
    }
}

export type RuntimeLogOptions = {
    service: string;
    version?: string;
    dir?: string;
    level?: RuntimeLevel;
    fileBytes?: number;
    totalBytes?: number;
    retentionDays?: number;
};

export function runtimeLogOptionsFromEnv(
    service: string,
    env: NodeJS.ProcessEnv = process.env,
): RuntimeLogOptions {
    const level = (env.LOG_LEVEL ?? '').toLowerCase();
    const mb = (value: string | undefined, fallback: number) => {
        const n = Number(value);
        return (Number.isFinite(n) && n > 0 ? n : fallback) * 1024 * 1024;
    };
    const days = Number(env.LOG_RETENTION_DAYS);
    return {
        service,
        version: packageVersion(),
        dir: env.LOG_DIR || undefined,
        level: level in ORDER ? (level as RuntimeLevel) : 'info',
        fileBytes: mb(env.LOG_FILE_MB, 20),
        totalBytes: mb(env.LOG_MAX_MB, 200),
        retentionDays: Number.isFinite(days) && days > 0 ? days : 14,
    };
}

export function redact(value: unknown, depth = 0): unknown {
    if (value === null || value === undefined) return value;
    if (typeof value === 'string') {
        return value.length > MAX_STRING
            ? `${value.slice(0, MAX_STRING)}…`
            : value;
    }
    if (typeof value !== 'object') return value;
    if (value instanceof Error) {
        return {
            name: value.name,
            message: value.message,
            stack: value.stack,
        };
    }
    if (value instanceof Date) return value.toISOString();
    if (depth >= MAX_DEPTH) return '[truncated]';
    if (Array.isArray(value)) {
        return value.slice(0, 50).map((item) => redact(item, depth + 1));
    }
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
        out[key] = SECRET_KEY.test(key)
            ? '[redacted]'
            : redact(item, depth + 1);
    }
    return out;
}

const pad = (n: number) => String(n).padStart(2, '0');
const stamp = (d: Date) =>
    `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}-` +
    `${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
const dayOf = (d: Date) => d.toISOString().slice(0, 10);

export class RuntimeLog {
    private stream: WriteStream | null = null;
    private size = 0;
    private day = dayOf(new Date());
    private readonly rank: number;
    private readonly path: string | null;
    private failed = false;
    private rotating = false;
    private readonly pending: string[] = [];

    constructor(private readonly options: RuntimeLogOptions) {
        this.rank = ORDER[options.level ?? 'info'];
        this.path = options.dir
            ? join(options.dir, `${options.service}.log`)
            : null;
        if (this.path && options.dir) {
            try {
                mkdirSync(options.dir, { recursive: true });
                this.size = safeSize(this.path);
                if (this.size > 0) {
                    this.day = dayOf(new Date(statSync(this.path).mtimeMs));
                }
                this.open();
                this.prune();
                setInterval(() => this.prune(), PRUNE_EVERY_MS).unref();
            } catch (error) {
                this.disable(error);
            }
        }
    }

    debug(msg: string, fields?: Record<string, unknown>) {
        this.write('debug', msg, fields);
    }
    info(msg: string, fields?: Record<string, unknown>) {
        this.write('info', msg, fields);
    }
    warn(msg: string, fields?: Record<string, unknown>) {
        this.write('warn', msg, fields);
    }
    error(msg: string, fields?: Record<string, unknown>) {
        this.write('error', msg, fields);
    }

    write(level: RuntimeLevel, msg: string, fields?: Record<string, unknown>) {
        if (ORDER[level] < this.rank) return;
        const now = new Date();
        const line =
            JSON.stringify({
                ts: now.toISOString(),
                level,
                service: this.options.service,
                ...(this.options.version
                    ? { version: this.options.version }
                    : {}),
                msg,
                ...((redact(fields ?? {}) as Record<string, unknown>) ?? {}),
            }) + '\n';
        if (!process.env.JEST_WORKER_ID) {
            (level === 'error' || level === 'warn'
                ? process.stderr
                : process.stdout
            ).write(line);
        }
        this.append(line, now);
    }

    close() {
        this.stream?.end();
        this.stream = null;
    }

    private open() {
        if (!this.path) return;
        const stream = createWriteStream(this.path, { flags: 'a' });
        stream.on('error', (error) => this.disable(error));
        this.stream = stream;
    }

    private disable(error: unknown) {
        if (this.failed) return;
        this.failed = true;
        this.stream = null;
        const message = error instanceof Error ? error.message : String(error);
        process.stderr.write(
            JSON.stringify({
                ts: new Date().toISOString(),
                level: 'error',
                service: this.options.service,
                ...(this.options.version
                    ? { version: this.options.version }
                    : {}),
                msg: 'runtime log file disabled',
                error: message,
            }) + '\n',
        );
    }

    private append(line: string, now: Date) {
        if (this.failed || !this.path) return;
        if (this.rotating) {
            this.pending.push(line);
            return;
        }
        if (!this.stream) return;
        const bytes = Buffer.byteLength(line);
        const fileBytes = this.options.fileBytes ?? 20 * 1024 * 1024;
        if (
            this.size > 0 &&
            (dayOf(now) !== this.day || this.size + bytes > fileBytes)
        ) {
            this.pending.push(line);
            this.rotate(now);
            return;
        }
        this.size += bytes;
        this.stream.write(line);
    }

    private rotate(now: Date) {
        const path = this.path;
        const stream = this.stream;
        if (!path || !stream) return;
        this.rotating = true;
        this.stream = null;
        const rotated = join(
            this.options.dir as string,
            `${this.options.service}-${stamp(now)}-${String(now.getUTCMilliseconds()).padStart(3, '0')}.log`,
        );
        stream.once('close', () => {
            try {
                renameSync(path, rotated);
                compress(rotated);
            } catch (error) {
                this.rotating = false;
                this.disable(error);
                return;
            }
            this.size = 0;
            this.day = dayOf(now);
            this.open();
            this.rotating = false;
            const queued = this.pending.splice(0);
            for (const queuedLine of queued) {
                this.size += Buffer.byteLength(queuedLine);
                this.stream?.write(queuedLine);
            }
            this.prune();
        });
        stream.end();
    }

    prune() {
        const dir = this.options.dir;
        if (!dir) return;
        try {
            pruneDir(dir, this.options.service, {
                retentionMs: (this.options.retentionDays ?? 14) * DAY_MS,
                totalBytes: this.options.totalBytes ?? 200 * 1024 * 1024,
            });
        } catch (error) {
            process.stderr.write(
                JSON.stringify({
                    ts: new Date().toISOString(),
                    level: 'warn',
                    service: this.options.service,
                    ...(this.options.version
                        ? { version: this.options.version }
                        : {}),
                    msg: 'runtime log prune failed',
                    error:
                        error instanceof Error ? error.message : String(error),
                }) + '\n',
            );
        }
    }
}

function safeSize(path: string): number {
    try {
        return statSync(path).size;
    } catch {
        return 0;
    }
}

function compress(path: string) {
    const target = `${path}.gz`;
    pipeline(
        createReadStream(path),
        createGzip(),
        createWriteStream(target),
        (error) => {
            try {
                if (error) {
                    unlinkSync(target);
                } else {
                    unlinkSync(path);
                }
            } catch {
                return;
            }
        },
    );
}

export function pruneDir(
    dir: string,
    service: string,
    limits: { retentionMs: number; totalBytes: number },
    now = Date.now(),
) {
    const files = readdirSync(dir)
        .filter((name) => name.startsWith(`${service}-`))
        .map((name) => {
            const stat = statSync(join(dir, name));
            return { name, size: stat.size, mtime: stat.mtimeMs };
        })
        .sort((a, b) => a.mtime - b.mtime);
    const current = safeSize(join(dir, `${service}.log`));
    let total = current + files.reduce((sum, file) => sum + file.size, 0);
    for (const file of files) {
        const expired = now - file.mtime > limits.retentionMs;
        if (!expired && total <= limits.totalBytes) continue;
        try {
            unlinkSync(join(dir, file.name));
            total -= file.size;
        } catch {
            continue;
        }
    }
}
