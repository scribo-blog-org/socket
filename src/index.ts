import 'dotenv/config';
import { AccessVerifier } from './access';
import { loadConfig } from './config';
import { Conversations } from './mongo';
import { start } from './server';

async function main() {
    const config = loadConfig();
    const access = new AccessVerifier(config.publicKey);
    const conversations = await Conversations.connect(config);
    const server = await start({ config, access, conversations });

    const shutdown = async () => {
        await server.close();
        await conversations.close();
        process.exit(0);
    };
    process.on('SIGTERM', () => {
        void shutdown();
    });
    process.on('SIGINT', () => {
        void shutdown();
    });
}

void main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
});
