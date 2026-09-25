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
    /** Number of shards to create on first boot (default: 1). */
    shards?: number;
    /** Max shards this instance will claim (default: 1). */
    maxLocalShards?: number;
    /** Shared worker concurrency budget across all owned shards (default: 100). */
    maxWorkers?: number;
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
    webhookUrl?: string;
    maxExecutionSeconds?: number;
    pool?: string;
    triggerAfterIds?: string[];
}

export type TaskHandler = (data: any) => Promise<void>;

export class SnerdQueue {
    private engine: ChildProcess;
    private handlers: Map<string, TaskHandler> = new Map();
    private maxRetryHandlers: Map<string, TaskHandler> = new Map();
    private isShuttingDown: boolean = false;
    private pendingEnqueues: Map<string, { resolve: () => void, reject: (err: Error) => void }> = new Map();
    private wsClients: Set<WebSocket> = new Set();
    /** Shard keys owned by this instance, updated from membership events. */
    private _ownedShards: string[] = [];
    /** Resolves when the daemon process has exited (used for drain). */
    private _exitPromise: Promise<void> = Promise.resolve();
    private _latestStats: any = { enqueued: 0, processed: 0, failed: 0, per_shard: [] };
    private _statsInterval: NodeJS.Timeout | null = null;

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

        // Pass sharding options as env vars for the daemon process.
        const env: NodeJS.ProcessEnv = { ...process.env };
        if (!env['HOSTNAME'])                      env['HOSTNAME']           = os.hostname();
        if (options?.shards !== undefined)         env['SNERD_SHARDS']       = String(options.shards);
        if (options?.maxLocalShards !== undefined) env['SNERD_MAX_SHARDS']   = String(options.maxLocalShards);
        if (options?.maxWorkers !== undefined)     env['SNERD_MAX_WORKERS']  = String(options.maxWorkers);

        this.engine = spawn(binPath, args, { stdio: ['pipe', 'pipe', 'pipe'], env });

        if (!this.engine.stdin || !this.engine.stdout || !this.engine.stderr) {
            throw new Error('[Snerd] Failed to initialize standard I/O pipes with the engine.');
        }

        // Track daemon exit for graceful drain.
        this._exitPromise = new Promise<void>(resolve => {
            this.engine.once('exit', () => resolve());
        });

        this.setupEventLoop();

    }

    private engineAlive: boolean = true;

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

        // Prevent EPIPE crash when daemon dies
        this.engine.stdin!.on('error', (err: any) => {
            if (err.code === 'EPIPE' || err.code === 'ERR_STREAM_DESTROYED') return;
            console.error(`[Snerd] stdin error: ${err.message}`);
        });

        this.engine.on('close', (code: number | null, signal: NodeJS.Signals | null) => {
            this.engineAlive = false;
            if (!this.isShuttingDown) {
                console.warn(`[Snerd] Engine process terminated unexpectedly with code ${code}, signal ${signal}.`);
            }
            // Reject all pending enqueue promises
            for (const [id, pending] of this.pendingEnqueues) {
                pending.reject(new Error(`[Snerd] Engine terminated before ack for task '${id}'`));
            }
            this.pendingEnqueues.clear();
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
        } else if (msg.action === 'membership') {
            // Informational only — daemon owns all routing.
            this._ownedShards = msg.owned ?? [];
            console.log(`[Snerd] Cluster: queue=${msg.queue} shards=${msg.shards} owned=[${this._ownedShards.join(', ')}] version=${msg.version}`);
        } else if (msg.action === 'stats') {
            console.log("[Snerd] Got stats from daemon:", msg);
            this._latestStats = {
                enqueued: msg.total_enqueued || 0,
                processed: msg.total_executed || 0,
                failed: msg.total_failed || 0,
                per_shard: msg.per_shard || []
            };
        } else if (msg.action === 'execute') {
            const handler = this.handlers.get(msg.task_type);
            
            if (!handler) {
                this.send({ action: 'result', task_id: msg.task_id, status: 'error', error_msg: 'No handler registered for this task type.' });
                return;
            }

            try {
                const parsedData = typeof msg.task_data === 'string' ? JSON.parse(msg.task_data) : msg.task_data;
                const executePromise = taskContext.run(msg.task_id, async () => {
                    await handler(parsedData);
                });
                
                if (msg.max_execution_seconds) {
                    const timeoutPromise = new Promise((_, reject) => {
                        setTimeout(() => reject(new Error(`Task execution timed out after ${msg.max_execution_seconds} seconds`)), msg.max_execution_seconds * 1000);
                    });
                    await Promise.race([executePromise, timeoutPromise]);
                } else {
                    await executePromise;
                }
                
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
                    // Pass full task info to the DLQ handler (not just data)
                    const dlqPayload = {
                        taskId: msg.task_id,
                        taskType: msg.task_type,
                        data: parsedData,
                    };
                    await taskContext.run(msg.task_id, async () => {
                        await handler(dlqPayload);
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
        if (this.engine.stdin && this.engineAlive) {
            try {
                this.engine.stdin.write(JSON.stringify(msg) + '\n');
            } catch (e: any) {
                if (e.code !== 'EPIPE' && e.code !== 'ERR_STREAM_DESTROYED') {
                    throw e;
                }
            }
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
        if (this.isShuttingDown) {
            return Promise.reject(new Error('[Snerd] Queue is shutting down; enqueue rejected.'));
        }
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
                cron: options.cron,
                webhook_url: options.webhookUrl,
                max_execution_seconds: options.maxExecutionSeconds,
                pool: options.pool,
                trigger_after_ids: options.triggerAfterIds
            });
        });
    }

    /** Returns the shard keys currently owned by this instance (from last membership event). */
    public get ownedShards(): string[] {
        return [...this._ownedShards];
    }

    /**
     * Graceful shutdown: signals the daemon to drain in-flight tasks, then
     * waits for the process to exit. New enqueues are rejected immediately.
     * The execute-response path stays alive so in-flight tasks can complete.
     */
    public async shutdown(): Promise<void> {
        if (this.isShuttingDown) return;
        this.isShuttingDown = true;

        if (this._statsInterval) {
            clearInterval(this._statsInterval);
            this._statsInterval = null;
        }

        // Send SIGTERM — triggers the daemon's graceful drain (pause → drain → release claims → exit).
        if (this.engineAlive) {
            this.engine.kill('SIGTERM');
        }

        // Wait for the daemon to exit, with a hard timeout as a safety net.
        const DRAIN_TIMEOUT_MS = 35_000;
        await Promise.race([
            this._exitPromise,
            new Promise<void>(resolve => setTimeout(resolve, DRAIN_TIMEOUT_MS)),
        ]);

        // Reject any enqueues that never got an ack.
        for (const [id, pending] of this.pendingEnqueues) {
            pending.reject(new Error(`[Snerd] Engine shut down before ack for task '${id}'`));
        }
        this.pendingEnqueues.clear();
    }

    public yieldProgress(data: string) {
        const taskId = taskContext.getStore();
        if (!taskId) {
            throw new Error('[Snerd] yieldProgress must be called within a task handler context.');
        }
        this.send({ action: 'progress', task_id: taskId, data });
    }

    public startDashboard(port: number = 8080) {
        if (!this._statsInterval) {
            this._statsInterval = setInterval(() => {
                this.send({ action: 'stats' });
            }, 2000);
            this.send({ action: 'stats' });
        }

        const server = http.createServer((req, res) => {
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
            res.setHeader('Pragma', 'no-cache');
            res.setHeader('Expires', '0');

            const storagePath = this.engine.spawnargs[1] || './.snerdata';

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
                    res.writeHead(200, { 'Content-Type': 'application/json', ...corsHeaders });
                    res.end(JSON.stringify(this._latestStats));
                } else if (req.url === '/api/membership') {
                    let membership = null;
                    const memPath = path.join(storagePath, 'membership.json');
                    if (fs.existsSync(memPath)) {
                        try {
                            membership = JSON.parse(fs.readFileSync(memPath, 'utf8'));
                        } catch(e) {}
                    }
                    res.writeHead(200, { 'Content-Type': 'application/json', ...corsHeaders });
                    res.end(JSON.stringify(membership || { claims: {} }));
                } else if (req.url === '/api/tasks') {
                    const tasksMap = new Map();
                    // Iterate over owned shards to aggregate tasks
                    const shardsToRead = this._ownedShards.length > 0 ? this._ownedShards : [''];
                    
                    for (const shard of shardsToRead) {
                        const tasksPath = shard ? path.join(storagePath, shard, 'tasks', 'tasks.log') : path.join(storagePath, 'tasks', 'tasks.log');
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
                    }
                    
                    const formatted = [];
                    for (const t of tasksMap.values()) {
                        let status: string;
                        if (t.deletedAt) {
                            if (t.LastJobError && (t.retryCount || 0) >= (t.maxRetries || 3)) {
                                status = 'dead_letter';
                            } else if (t.LastJobError) {
                                status = 'failed';
                            } else {
                                status = 'completed';
                            }
                        } else if (t.LastJobError) {
                            status = 'failed';
                        } else {
                            const execTime = t.executeAt ? new Date(t.executeAt).getTime() : 0;
                            status = (execTime > 0 && execTime <= Date.now()) ? 'active' : 'queued';
                        }
                        formatted.push({
                            id: t.taskId,
                            type: t.taskType,
                            status,
                            progress: 0,
                            retryCount: t.retryCount || 0,
                            maxRetries: t.maxRetries || 3,
                            retryAfterTime: t.retryAfterTime,
                            cronExpression: t.cronExpression || null,
                            webhookUrl: t.webhookUrl || null,
                            maxExecutionSeconds: t.maxExecutionSeconds || null
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
