import http from 'http';
import { randomUUID } from 'crypto';
import Redis from 'ioredis';
import { WebSocket, WebSocketServer } from 'ws';
import { AccessVerifier } from './access';
import type { SocketConfig } from './config';
import { Conversations } from './mongo';
import { PRESENCE_CHANNEL, PresenceStore } from './presence';

const EVENTS_CHANNEL = 'scribo:events';
const PRESENCE_REFRESH_MS = 15000;

type ClientState = {
    userId: string | null;
    rooms: Set<string>;
};

type Control =
    | { type: 'auth'; access?: unknown }
    | { type: 'subscribe'; room?: unknown }
    | { type: 'unsubscribe'; room?: unknown }
    | { type: 'presence:query'; id?: unknown; users?: unknown };

export type SocketServer = {
    close: () => Promise<void>;
};

function parseRoom(room: string): { kind: 'user' | 'chat'; id: string } | null {
    const match = /^(user|chat):([^\s:]+)$/.exec(room);
    if (!match) return null;
    return { kind: match[1] as 'user' | 'chat', id: match[2] };
}

function send(socket: WebSocket, body: unknown) {
    if (socket.readyState !== WebSocket.OPEN) return;
    socket.send(JSON.stringify(body));
}

function presenceUserIds(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const ids: string[] = [];
    for (const item of value) {
        if (typeof item !== 'string') continue;
        const id = item.trim();
        if (!/^[a-zA-Z0-9_-]{1,64}$/.test(id)) continue;
        ids.push(id);
        if (ids.length >= 100) break;
    }
    return ids;
}

export async function start(options: {
    config: SocketConfig;
    access: AccessVerifier;
    conversations: Conversations;
}): Promise<SocketServer> {
    const { config, access, conversations } = options;
    const rooms = new Map<string, Set<WebSocket>>();

    const join = (room: string, socket: WebSocket, state: ClientState) => {
        let members = rooms.get(room);
        if (!members) {
            members = new Set();
            rooms.set(room, members);
        }
        members.add(socket);
        state.rooms.add(room);
    };

    const leave = (room: string, socket: WebSocket, state: ClientState) => {
        state.rooms.delete(room);
        const members = rooms.get(room);
        if (!members) return;
        members.delete(socket);
        if (members.size === 0) rooms.delete(room);
    };

    const dropSocket = (socket: WebSocket, state: ClientState) => {
        for (const room of [...state.rooms]) {
            leave(room, socket, state);
        }
    };

    const redis = new Redis(config.redisUrl, {
        maxRetriesPerRequest: 3,
    });
    const subscriber = new Redis(config.redisUrl, {
        maxRetriesPerRequest: null,
    });
    const presence = new PresenceStore(redis);
    const authed = new Set<WebSocket>();
    redis.on('error', (error: Error) => {
        console.error(`redis ${error.message}`);
    });
    subscriber.on('error', (error: Error) => {
        console.error(`redis ${error.message}`);
    });
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
            () => reject(new Error('redis connection timed out')),
            10000,
        );
        let ready = 0;
        const done = () => {
            ready += 1;
            if (ready < 2) return;
            clearTimeout(timer);
            resolve();
        };
        redis.once('ready', done);
        subscriber.once('ready', done);
    });
    await subscriber.subscribe(EVENTS_CHANNEL, PRESENCE_CHANNEL);

    const server = http.createServer((req, res) => {
        const path = req.url?.split('?')[0];
        if (req.method === 'GET' && path === '/health') {
            res.writeHead(200, { 'content-type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
            return;
        }
        res.writeHead(404);
        res.end();
    });

    const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

    subscriber.on('message', (channel, message) => {
        if (channel === PRESENCE_CHANNEL) {
            let parsed: { userId?: unknown; online?: unknown };
            try {
                parsed = JSON.parse(message) as typeof parsed;
            } catch {
                return;
            }
            if (typeof parsed.userId !== 'string' || typeof parsed.online !== 'boolean') {
                return;
            }
            const body = {
                type: 'presence',
                user: parsed.userId,
                online: parsed.online,
            };
            for (const socket of authed) {
                send(socket, body);
            }
            return;
        }
        if (channel !== EVENTS_CHANNEL) return;
        let parsed: { room?: unknown; event?: unknown; payload?: unknown };
        try {
            parsed = JSON.parse(message) as typeof parsed;
        } catch {
            return;
        }
        if (typeof parsed.room !== 'string' || typeof parsed.event !== 'string') {
            return;
        }
        const members = rooms.get(parsed.room);
        if (!members) return;
        const body = {
            room: parsed.room,
            event: parsed.event,
            payload: parsed.payload ?? {},
        };
        for (const socket of members) {
            send(socket, body);
        }
    });

    wss.on('connection', (socket) => {
        const state: ClientState = { userId: null, rooms: new Set() };
        const connectionId = randomUUID();
        let closed = false;
        let presenceJoined = false;
        let presenceTimer: ReturnType<typeof setInterval> | null = null;
        let chain = Promise.resolve();
        const authTimer = setTimeout(() => {
            if (!state.userId) socket.close(4001, 'unauthorized');
        }, 10000);

        const markOnline = async (userId: string) => {
            if (closed) return;
            const becameOnline = presenceJoined
                ? await presence.touch(userId, connectionId)
                : await presence.join(userId, connectionId);
            if (closed) {
                if (!presenceJoined) {
                    await presence.leave(userId, connectionId);
                }
                return;
            }
            presenceJoined = true;
            if (becameOnline) await presence.publish(userId, true);
        };

        const markOffline = async () => {
            if (presenceTimer) {
                clearInterval(presenceTimer);
                presenceTimer = null;
            }
            authed.delete(socket);
            const userId = state.userId;
            if (!userId || !presenceJoined) return;
            presenceJoined = false;
            const becameOffline = await presence.leave(userId, connectionId);
            if (becameOffline) await presence.publish(userId, false);
        };

        const handle = async (raw: string) => {
            let message: Control;
            try {
                message = JSON.parse(raw) as Control;
            } catch {
                send(socket, { type: 'error', error: 'bad_request' });
                return;
            }

            if (message.type === 'auth') {
                if (typeof message.access !== 'string' || !message.access) {
                    send(socket, { type: 'error', error: 'unauthorized' });
                    return;
                }
                const userId = access.userId(message.access);
                if (!userId) {
                    send(socket, { type: 'error', error: 'unauthorized' });
                    return;
                }
                if (state.userId && state.userId !== userId) {
                    send(socket, { type: 'error', error: 'forbidden' });
                    return;
                }
                state.userId = userId;
                clearTimeout(authTimer);
                authed.add(socket);
                try {
                    await markOnline(userId);
                } catch (error) {
                    console.error(
                        error instanceof Error ? error.message : error,
                    );
                }
                if (!presenceTimer) {
                    presenceTimer = setInterval(() => {
                        if (!state.userId) return;
                        void markOnline(state.userId).catch((error: unknown) => {
                            console.error(
                                error instanceof Error ? error.message : error,
                            );
                        });
                    }, PRESENCE_REFRESH_MS);
                }
                send(socket, { type: 'auth', ok: true });
                return;
            }

            if (message.type === 'presence:query') {
                if (!state.userId) {
                    send(socket, { type: 'error', error: 'unauthorized' });
                    return;
                }
                const users = presenceUserIds(message.users);
                try {
                    const online = await presence.online(users);
                    send(socket, {
                        type: 'presence',
                        id: typeof message.id === 'string' ? message.id : undefined,
                        users: online,
                    });
                } catch (error) {
                    console.error(
                        error instanceof Error ? error.message : error,
                    );
                    send(socket, { type: 'error', error: 'unavailable' });
                }
                return;
            }

            if (!state.userId) {
                send(socket, { type: 'error', error: 'unauthorized' });
                return;
            }

            if (message.type === 'subscribe') {
                if (typeof message.room !== 'string') {
                    send(socket, { type: 'error', error: 'bad_request' });
                    return;
                }
                const room = parseRoom(message.room);
                if (!room) {
                    send(socket, {
                        type: 'error',
                        error: 'forbidden',
                        room: message.room,
                    });
                    return;
                }
                if (room.kind === 'user') {
                    if (room.id !== state.userId) {
                        send(socket, {
                            type: 'error',
                            error: 'forbidden',
                            room: message.room,
                        });
                        return;
                    }
                    join(`user:${room.id}`, socket, state);
                    send(socket, { type: 'subscribe', ok: true, room: message.room });
                    return;
                }
                try {
                    const allowed = await conversations.hasParticipant(
                        room.id,
                        state.userId,
                    );
                    if (!allowed) {
                        send(socket, {
                            type: 'error',
                            error: 'forbidden',
                            room: message.room,
                        });
                        return;
                    }
                } catch (error) {
                    console.error(
                        error instanceof Error ? error.message : error,
                    );
                    send(socket, {
                        type: 'error',
                        error: 'unavailable',
                        room: message.room,
                    });
                    return;
                }
                join(`chat:${room.id}`, socket, state);
                send(socket, { type: 'subscribe', ok: true, room: message.room });
                return;
            }

            if (message.type === 'unsubscribe') {
                if (typeof message.room !== 'string') {
                    send(socket, { type: 'error', error: 'bad_request' });
                    return;
                }
                leave(message.room, socket, state);
                send(socket, {
                    type: 'unsubscribe',
                    ok: true,
                    room: message.room,
                });
                return;
            }

            send(socket, { type: 'error', error: 'bad_request' });
        };

        socket.on('message', (data, isBinary) => {
            if (isBinary) {
                send(socket, { type: 'error', error: 'bad_request' });
                return;
            }
            const raw = data.toString();
            chain = chain.then(() => handle(raw)).catch((error: unknown) => {
                console.error(error instanceof Error ? error.message : error);
                send(socket, { type: 'error', error: 'unavailable' });
            });
        });

        socket.on('close', () => {
            closed = true;
            clearTimeout(authTimer);
            dropSocket(socket, state);
            void markOffline().catch((error: unknown) => {
                console.error(error instanceof Error ? error.message : error);
            });
        });
    });

    await new Promise<void>((resolve) => {
        server.listen(config.port, '0.0.0.0', () => {
            console.log(
                `socket ready port=${config.port} redis connected mongo connected host=${config.dbHost} db=${config.dbName}`,
            );
            resolve();
        });
    });

    return {
        close: async () => {
            wss.clients.forEach((socket) => socket.close());
            await new Promise<void>((resolve, reject) => {
                server.close((error) => (error ? reject(error) : resolve()));
            });
            await subscriber.quit();
            await redis.quit();
        },
    };
}
