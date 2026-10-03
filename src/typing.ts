import type Redis from 'ioredis';

export const TYPING_TTL_MS = 2000;

export type TypingItem = {
    conversationId: string;
    userId: string;
};

const PULSE = `
local gate = redis.call('GET', KEYS[1])
if gate and tonumber(ARGV[1]) <= tonumber(gate) then
  return 0
end
local ttl = tonumber(ARGV[3])
redis.call('SET', KEYS[2], ARGV[2], 'PX', ttl)
redis.call('SADD', KEYS[3], ARGV[2])
redis.call('PEXPIRE', KEYS[3], ttl)
redis.call('SADD', KEYS[4], ARGV[4])
redis.call('PEXPIRE', KEYS[4], 15000)
redis.call('DEL', KEYS[5])
local peers = cjson.decode(ARGV[5])
for _, peer in ipairs(peers) do
  redis.call('SADD', KEYS[5], peer)
  redis.call('SADD', 'typing:inbox:' .. peer, ARGV[6])
  redis.call('PEXPIRE', 'typing:inbox:' .. peer, 15000)
end
if #peers > 0 then
  redis.call('PEXPIRE', KEYS[5], 15000)
end
return 1
`;

const STOP = `
local peers = redis.call('SMEMBERS', KEYS[5])
redis.call('SET', KEYS[1], ARGV[1], 'PX', 5000)
redis.call('DEL', KEYS[2], KEYS[3], KEYS[5])
redis.call('SREM', KEYS[4], ARGV[2])
for _, peer in ipairs(peers) do
  redis.call('SREM', 'typing:inbox:' .. peer, ARGV[3])
end
return peers
`;

const DISCONNECT = `
redis.call('SREM', KEYS[3], ARGV[1])
if redis.call('SCARD', KEYS[3]) > 0 then
  return {}
end
local active = redis.call('EXISTS', KEYS[2])
local peers = redis.call('SMEMBERS', KEYS[5])
redis.call('DEL', KEYS[2], KEYS[3], KEYS[5])
redis.call('SREM', KEYS[4], ARGV[2])
for _, peer in ipairs(peers) do
  redis.call('SREM', 'typing:inbox:' .. peer, ARGV[3])
end
if active == 1 then
  return peers
end
return {}
`;

export class TypingStore {
    constructor(private readonly redis: Redis) {}

    private activeKey(conversationId: string, userId: string) {
        return `typing:active:${conversationId}:${userId}`;
    }

    private connsKey(conversationId: string, userId: string) {
        return `typing:conns:${conversationId}:${userId}`;
    }

    private peersKey(conversationId: string, userId: string) {
        return `typing:peers:${conversationId}:${userId}`;
    }

    private userKey(userId: string) {
        return `typing:user:${userId}`;
    }

    private inboxKey(userId: string) {
        return `typing:inbox:${userId}`;
    }

    private gateKey(conversationId: string, userId: string) {
        return `typing:gate:${conversationId}:${userId}`;
    }

    private member(conversationId: string, userId: string) {
        return `${conversationId}:${userId}`;
    }

    private keys(conversationId: string, userId: string) {
        return [
            this.gateKey(conversationId, userId),
            this.activeKey(conversationId, userId),
            this.connsKey(conversationId, userId),
            this.userKey(userId),
            this.peersKey(conversationId, userId),
        ];
    }

    async pulse(
        conversationId: string,
        userId: string,
        connectionId: string,
        peerIds: string[],
        at: number,
    ): Promise<boolean> {
        const keys = this.keys(conversationId, userId);
        const accepted = await this.redis.eval(
            PULSE,
            keys.length,
            ...keys,
            String(at),
            connectionId,
            String(TYPING_TTL_MS),
            conversationId,
            JSON.stringify(peerIds),
            this.member(conversationId, userId),
        );
        return Number(accepted) === 1;
    }

    async stop(
        conversationId: string,
        userId: string,
        at: number,
    ): Promise<string[]> {
        const keys = this.keys(conversationId, userId);
        const peers = await this.redis.eval(
            STOP,
            keys.length,
            ...keys,
            String(at),
            conversationId,
            this.member(conversationId, userId),
        );
        return Array.isArray(peers) ? peers.map(String) : [];
    }

    async disconnect(
        userId: string,
        connectionId: string,
    ): Promise<Array<{ conversationId: string; peers: string[] }>> {
        const conversationIds = await this.redis.smembers(this.userKey(userId));
        const stopped: Array<{ conversationId: string; peers: string[] }> = [];

        for (const conversationId of conversationIds) {
            const keys = this.keys(conversationId, userId);
            const peers = await this.redis.eval(
                DISCONNECT,
                keys.length,
                ...keys,
                connectionId,
                conversationId,
                this.member(conversationId, userId),
            );
            if (!Array.isArray(peers) || peers.length === 0) continue;
            stopped.push({
                conversationId,
                peers: peers.map(String),
            });
        }

        return stopped;
    }

    async listFor(userId: string): Promise<TypingItem[]> {
        const members = await this.redis.smembers(this.inboxKey(userId));
        if (!members.length) return [];

        const parsed: TypingItem[] = [];
        const pipe = this.redis.pipeline();
        for (const member of members) {
            const sep = member.indexOf(':');
            if (sep <= 0) continue;
            const conversationId = member.slice(0, sep);
            const typerId = member.slice(sep + 1);
            parsed.push({ conversationId, userId: typerId });
            pipe.exists(this.activeKey(conversationId, typerId));
        }
        if (!parsed.length) return [];

        const rows = await pipe.exec();
        const live: TypingItem[] = [];
        const stale: string[] = [];
        parsed.forEach((item, index) => {
            const exists = Number(rows?.[index]?.[1] ?? 0) === 1;
            if (exists) live.push(item);
            else stale.push(this.member(item.conversationId, item.userId));
        });
        if (stale.length) {
            await this.redis.srem(this.inboxKey(userId), ...stale);
        }
        return live;
    }
}
