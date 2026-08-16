import { spawn, ChildProcess } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';
import { AsyncLocalStorage } from 'async_hooks';
import * as http from 'http';
import { WebSocketServer, WebSocket } from 'ws';

const taskContext = new AsyncLocalStorage<string>();


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
    urgencyScore?: number;
    executeAt?: string | Date;
    cron?: string;
}

export type TaskHandler = (data: any) => Promise<void>;

export class SnerdQueue {
    private engine: ChildProcess;
    private handlers: Map<string, TaskHandler> = new Map();
    private maxRetryHandlers: Map<string, TaskHandler> = new Map();
    private isShuttingDown: boolean = false;
    private pendingEnqueues: Map<string, { resolve: () => void, reject: (err: Error) => void }> = new Map();
    private wsClients: Set<WebSocket> = new Set();

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
                await taskContext.run(msg.task_id, async () => {
                    await handler(parsedData);
                });
                this.send({ action: 'result', task_id: msg.task_id, status: 'success' });
            } catch (error: any) {
                this.send({ action: 'result', task_id: msg.task_id, status: 'error', error_msg: error.message || 'Unknown error during execution.' });
            }
        } else if (msg.action === 'progress') {
            for (const client of this.wsClients) {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify(msg));
                }
            }
        } else if (msg.action === 'max_retries_reached') {
            const handler = this.maxRetryHandlers.get(msg.task_type);
            if (handler) {
                try {
                    const parsedData = typeof msg.task_data === 'string' ? JSON.parse(msg.task_data) : msg.task_data;
                    await taskContext.run(msg.task_id, async () => {
                        await handler(parsedData);
                    });
                } catch (error: any) {
                    console.error(`[Snerd] Error in max retry handler for task ${msg.task_id}: ${error.message}`);
                }
            } else {
                console.warn(`[Snerd] Dead Letter Queue: Task ${msg.task_id} (${msg.task_type}) permanently failed after max retries.`);
            }
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

    public registerMaxRetryHandler(taskType: string, handler: TaskHandler) {
        this.maxRetryHandlers.set(taskType, handler);
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
                auto_dedupe: options.autoDedupe,
                urgency_score: options.urgencyScore,
                execute_at: options.executeAt instanceof Date ? options.executeAt.toISOString() : options.executeAt,
                cron: options.cron
            });
        });
    }

    public shutdown() {
        if (this.isShuttingDown) return;
        this.isShuttingDown = true;
        this.engine.kill('SIGINT');
    }

    public yieldProgress(data: string) {
        const taskId = taskContext.getStore();
        if (!taskId) {
            throw new Error('[Snerd] yieldProgress must be called within a task handler context.');
        }
        this.send({ action: 'progress', task_id: taskId, data });
    }

    public startDashboard(port: number = 8080) {
        const server = http.createServer((req, res) => {
            const storagePath = this.engine.spawnargs[1] || './.snerdata';
            const tasksPath = path.join(storagePath, 'tasks', 'tasks.log');

            const corsHeaders = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
            };

            if (req.method === 'OPTIONS') {
                res.writeHead(204, corsHeaders);
                res.end();
                return;
            }

            if (req.method === 'GET') {
                if (req.url === '/') {
                    const htmlPath = path.join(__dirname, '..', 'static', 'index.html');
                    if (fs.existsSync(htmlPath)) {
                        res.writeHead(200, { 'Content-Type': 'text/html' });
                        res.end(fs.readFileSync(htmlPath));
                    } else {
                        res.writeHead(404);
                        res.end('Dashboard UI not found in static folder.');
                    }
                } else if (req.url === '/api/stats') {
                    const stats = { enqueued: 0, processed: 0, failed: 0 };
                    if (fs.existsSync(tasksPath)) {
                        try {
                            const content = fs.readFileSync(tasksPath, 'utf8');
                            for (const line of content.split('\n')) {
                                if (!line.trim()) continue;
                                const t = JSON.parse(line);
                                stats.enqueued++;
                                if (t.deletedAt) {
                                    if (t.lastJobError) stats.failed++;
                                    else stats.processed++;
                                }
                            }
                        } catch(e) {}
                    }
                    res.writeHead(200, { 'Content-Type': 'application/json', ...corsHeaders });
                    res.end(JSON.stringify(stats));
                } else if (req.url === '/api/tasks') {
                    const tasksMap = new Map();
                    if (fs.existsSync(tasksPath)) {
                        try {
                            const content = fs.readFileSync(tasksPath, 'utf8');
                            for (const line of content.split('\n')) {
                                if (!line.trim()) continue;
                                const t = JSON.parse(line);
                                tasksMap.set(t.taskId, t);
                            }
                        } catch(e) {}
                    }
                    
                    const formatted = [];
                    for (const t of tasksMap.values()) {
                        let status = 'queued';
                        if (t.deletedAt) {
                            status = t.lastJobError ? 'failed' : 'completed';
                        } else {
                            status = t.lastJobError ? 'failed' : 'queued';
                        }
                        formatted.push({
                            id: t.taskId,
                            type: t.taskType,
                            status,
                            progress: 0,
                            retryCount: t.retryCount || 0,
                            maxRetries: t.maxRetries || 3,
                            retryAfterTime: t.retryAfterTime
                        });
                    }
                    res.writeHead(200, { 'Content-Type': 'application/json', ...corsHeaders });
                    res.end(JSON.stringify(formatted.slice(0, 50)));
                } else {
                    res.writeHead(404);
                    res.end();
                }
            }
        });

        const wss = new WebSocketServer({ server });
        wss.on('connection', (ws) => {
            this.wsClients.add(ws);
            ws.on('close', () => this.wsClients.delete(ws));
        });

        server.listen(port, () => {
            console.log(`[Snerd] Dashboard running on http://localhost:${port}`);
        });
    }

}
