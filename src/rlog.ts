import { RuntimeLog, runtimeLogOptionsFromEnv } from './runtime-log';

export const rlog = new RuntimeLog(runtimeLogOptionsFromEnv('socket'));

export function installProcessHandlers() {
    process.on('unhandledRejection', (reason) => {
        rlog.error('unhandledRejection', { error: reason });
    });
    process.on('uncaughtException', (error) => {
        rlog.error('uncaughtException', { error });
        setTimeout(() => process.exit(1), 100).unref();
    });
}
