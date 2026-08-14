import { spawn, ChildProcess } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';

export interface SnerdQueueOptions {
    binaryPath?: string;
    storagePath?: string;
}

export interface EnqueueOptions {
    id: string;
    type: string;
    data: any;
    maxRetries?: number;
    retryAfterHours?: number;
    rateLimitGroup?: string;
    maxPerMinute?: number;
    autoDedupe?: boolean;
}

export type TaskHandler = (data: any) => Promise<void>;

export class SnerdQueue {
    private engine: ChildProcess;
    private handlers: Map<string, TaskHandler> = new Map();
    private isShuttingDown: boolean = false;
    private pendingEnqueues: Map<string, { resolve: () => void, reject: (err: Error) => void }> = new Map();

    constructor(options?: SnerdQueueOptions) {
        let binPath = options?.binaryPath;

        if (!binPath) {
            // Attempt to use the downloaded binary from postinstall
            const ext = os.platform() === 'win32' ? '.exe' : '';
            binPath = path.join(__dirname, '..', 'bin', `snerdmq${ext}`);
        }

        if (!fs.existsSync(binPath)) {
            throw new Error(`[Snerd] Binary not found at ${binPath}. Ensure it is compiled or installed.`);
        }

        const args: string[] = [];
        if (options?.storagePath) {
            args.push(options.storagePath);
        }

        this.engine = spawn(binPath, args, { stdio: ['pipe', 'pipe', 'pipe'] });

        if (!this.engine.stdin || !this.engine.stdout || !this.engine.stderr) {
            throw new Error('[Snerd] Failed to initialize standard I/O pipes with the engine.');
        }

        this.setupEventLoop();

        // Graceful shutdown
        process.on('SIGINT', this.shutdown.bind(this));
        process.on('SIGTERM', this.shutdown.bind(this));
        process.on('exit', this.shutdown.bind(this));
    }

    private setupEventLoop() {
        let buffer = '';

        this.engine.stdout!.on('data', (data: Buffer) => {
            buffer += data.toString();
            let newlineIndex;

            while ((newlineIndex = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, newlineIndex).trim();
                buffer = buffer.slice(newlineIndex + 1);

                if (!line) continue;

                try {
                    const msg = JSON.parse(line);
                    this.handleEngineMessage(msg);
                } catch (e) {
                    // Ignore non-JSON stdout (e.g., Rust logs or warnings)
                }
            }
        });

        this.engine.stderr!.on('data', (data: Buffer) => {
            console.error(`[Snerd Engine Error]: ${data.toString().trim()}`);
        });

        this.engine.on('close', (code: number | null) => {
            if (!this.isShuttingDown) {
                console.warn(`[Snerd] Engine process terminated unexpectedly with code ${code}.`);
            }
        });
    }

    private async handleEngineMessage(msg: any) {
        if (msg.action === 'ack') {
            if (msg.task_id) {
                const pending = this.pendingEnqueues.get(msg.task_id);
                if (pending) {
                    pending.resolve();
                    this.pendingEnqueues.delete(msg.task_id);
                }
            }
        } else if (msg.action === 'error') {
            if (msg.task_id) {
                const pending = this.pendingEnqueues.get(msg.task_id);
                if (pending) {
                    pending.reject(new Error(msg.message));
                    this.pendingEnqueues.delete(msg.task_id);
                }
            } else {
                console.error(`[Snerd] Error from engine: ${msg.message}`);
            }
        } else if (msg.action === 'execute') {            const handler = this.handlers.get(msg.task_type);
            
            if (!handler) {
                this.send({ action: 'result', task_id: msg.task_id, status: 'error', error_msg: 'No handler registered for this task type.' });
                return;
            }

            try {
                const parsedData = typeof msg.task_data === 'string' ? JSON.parse(msg.task_data) : msg.task_data;
                await handler(parsedData);
                this.send({ action: 'result', task_id: msg.task_id, status: 'success' });
            } catch (error: any) {
                this.send({ action: 'result', task_id: msg.task_id, status: 'error', error_msg: error.message || 'Unknown error during execution.' });
            }
        } else if (msg.action === 'max_retries_reached') {
            console.warn(`[Snerd] Dead Letter Queue: Task ${msg.task_id} (${msg.task_type}) permanently failed after max retries.`);
        }
    }

    private send(msg: any) {
        if (this.engine.stdin && !this.isShuttingDown) {
            this.engine.stdin.write(JSON.stringify(msg) + '\n');
        }
    }

    public registerHandler(taskType: string, handler: TaskHandler) {
        this.handlers.set(taskType, handler);
        this.send({ action: 'register', task_type: taskType });
    }

    public enqueue(options: EnqueueOptions): Promise<void> {
        return new Promise((resolve, reject) => {
            this.pendingEnqueues.set(options.id, { resolve, reject });
            this.send({
                action: 'enqueue',
                task_id: options.id,
                task_type: options.type,
                task_data: JSON.stringify(options.data),
                max_retries: options.maxRetries ?? 3,
                retry_after_hours: options.retryAfterHours ?? 0.0,
                rate_limit_group: options.rateLimitGroup,
                max_per_minute: options.maxPerMinute,
                auto_dedupe: options.autoDedupe
            });
        });
    }

    public shutdown() {
        if (this.isShuttingDown) return;
        this.isShuttingDown = true;
        this.engine.kill('SIGINT');
    }
}
