import type Redis from 'ioredis';

export const PRESENCE_CHANNEL = 'scribo:presence';

const TTL_SEC = 45;

export class PresenceStore {
    constructor(private readonly redis: Redis) {}

    private key(userId: string) {
        return `presence:user:${userId}`;
    }

    async join(userId: string, connectionId: string): Promise<boolean> {
        const key = this.key(userId);
        const before = await this.redis.scard(key);
        await this.redis.sadd(key, connectionId);
        await this.redis.expire(key, TTL_SEC);
        return before === 0;
    }

    async touch(userId: string, connectionId: string): Promise<boolean> {
        const key = this.key(userId);
        const added = await this.redis.sadd(key, connectionId);
        await this.redis.expire(key, TTL_SEC);
        if (added === 0) return false;
        return (await this.redis.scard(key)) === 1;
    }

    async leave(userId: string, connectionId: string): Promise<boolean> {
        const key = this.key(userId);
        await this.redis.srem(key, connectionId);
        const left = await this.redis.scard(key);
        if (left === 0) {
            await this.redis.del(key);
            return true;
        }
        return false;
    }

    async online(userIds: string[]): Promise<Record<string, boolean>> {
        const unique = [...new Set(userIds)].slice(0, 100);
        const out: Record<string, boolean> = {};
        if (!unique.length) return out;

        const pipeline = this.redis.pipeline();
        for (const id of unique) pipeline.scard(this.key(id));
        const rows = await pipeline.exec();
        unique.forEach((id, index) => {
            const count = Number(rows?.[index]?.[1] ?? 0);
            out[id] = count > 0;
        });
        return out;
    }

    async publish(
        userId: string,
        online: boolean,
        at?: Date,
    ): Promise<void> {
        await this.redis.publish(
            PRESENCE_CHANNEL,
            JSON.stringify({
                userId,
                online,
                ...(at ? { at: at.toISOString() } : {}),
            }),
        );
    }
}
