import { MongoClient, ObjectId } from 'mongodb';
import type { SocketConfig } from './config';

function dbHost(value: string): string {
    const withoutScheme = value.trim().replace(/^mongodb(?:\+srv)?:\/\//, '');
    return withoutScheme.split('/')[0].split('?')[0];
}

export function mongoUri(config: Pick<
    SocketConfig,
    'mongoUri' | 'dbUser' | 'dbPassword' | 'dbHost' | 'dbName'
>): string {
    if (config.mongoUri) return config.mongoUri;
    const host = dbHost(config.dbHost);
    if (!host) {
        throw new Error('Set DB_HOST');
    }
    const userPart = encodeURIComponent(config.dbUser);
    const passwordPart = encodeURIComponent(config.dbPassword);
    const dbPart = encodeURIComponent(config.dbName);
    return `mongodb+srv://${userPart}:${passwordPart}@${host}/${dbPart}?retryWrites=true&w=majority`;
}

export class Conversations {
    constructor(
        private readonly client: MongoClient,
        private readonly dbName: string,
    ) {}

    static async connect(config: SocketConfig): Promise<Conversations> {
        const client = new MongoClient(mongoUri(config), {
            serverSelectionTimeoutMS: 10000,
        });
        await client.connect();
        await client.db(config.dbName).command({ ping: 1 });
        return new Conversations(client, config.dbName);
    }

    async hasParticipant(
        conversationId: string,
        userId: string,
    ): Promise<boolean> {
        if (!/^[a-fA-F0-9]{24}$/.test(conversationId)) return false;
        const doc = await this.client
            .db(this.dbName)
            .collection('conversations')
            .findOne(
                { _id: new ObjectId(conversationId) },
                { projection: { participants: 1 } },
            );
        if (!Array.isArray(doc?.participants)) return false;
        return doc.participants.some(
            (participant) => String(participant) === userId,
        );
    }

    async otherParticipants(
        conversationId: string,
        userId: string,
    ): Promise<string[] | null> {
        if (!/^[a-fA-F0-9]{24}$/.test(conversationId)) return null;
        const doc = await this.client
            .db(this.dbName)
            .collection('conversations')
            .findOne(
                { _id: new ObjectId(conversationId) },
                { projection: { participants: 1 } },
            );
        if (!Array.isArray(doc?.participants)) return null;
        const ids = doc.participants.map((participant) => String(participant));
        if (!ids.includes(userId)) return null;
        return ids.filter((id) => id !== userId);
    }

    async touchLastActivity(
        userId: string,
        at = new Date(),
    ): Promise<{ at: Date; isPublic: boolean } | null> {
        if (!/^[a-fA-F0-9]{24}$/.test(userId)) return null;
        const updated = await this.client
            .db(this.dbName)
            .collection('users')
            .findOneAndUpdate(
                { _id: new ObjectId(userId) },
                { $set: { last_activity_at: at } },
                {
                    returnDocument: 'after',
                    projection: { is_last_activity_public: 1 },
                },
            );
        if (!updated) return null;
        return {
            at,
            isPublic: updated.is_last_activity_public !== false,
        };
    }

    async close() {
        await this.client.close();
    }
}
