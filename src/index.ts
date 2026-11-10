import 'dotenv/config';
import { AccessVerifier } from './access';
import { loadConfig } from './config';
import { Conversations } from './mongo';
import { installProcessHandlers, rlog } from './rlog';
import { start } from './server';

async function main() {
    installProcessHandlers();
    const config = loadConfig();
    const access = new AccessVerifier(config.publicKey);
    const conversations = await Conversations.connect(config);
    rlog.info('mongo connected', {
        host: config.dbHost || 'local-uri',
        db: config.dbName,
    });
    const server = await start({ config, access, conversations });

    const shutdown = async (signal: string) => {
        rlog.info('shutdown', { signal });
        await server.close();
        await conversations.close();
        rlog.close();
        process.exit(0);
    };
    process.on('SIGTERM', () => {
        void shutdown('SIGTERM');
    });
    process.on('SIGINT', () => {
        void shutdown('SIGINT');
    });
}

void main().catch((error: unknown) => {
    rlog.error('socket failed to start', { error });
    setTimeout(() => process.exit(1), 100);
});
