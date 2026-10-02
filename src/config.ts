export type SocketConfig = {
    port: number;
    redisUrl: string;
    publicKey: string;
    mongoUri: string;
    dbUser: string;
    dbPassword: string;
    dbHost: string;
    dbName: string;
};

function required(env: NodeJS.ProcessEnv, key: string): string {
    const value = env[key]?.trim();
    if (!value) {
        throw new Error(`Set ${key}`);
    }
    return value;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): SocketConfig {
    if (env.JWT_PRIVATE_KEY?.trim() || env.JWT_REFRESH_KEY?.trim()) {
        throw new Error(
            'Socket must not receive JWT_PRIVATE_KEY or JWT_REFRESH_KEY',
        );
    }

    const port = Number(env.PORT || 3002);
    if (!Number.isInteger(port) || port <= 0) {
        throw new Error('Set PORT');
    }

    const mongoUri = env.MONGODB_URI?.trim() ?? '';
    return {
        port,
        redisUrl: required(env, 'REDIS_URL'),
        publicKey: required(env, 'JWT_PUBLIC_KEY'),
        mongoUri,
        dbUser: mongoUri ? '' : required(env, 'DB_USER'),
        dbPassword: mongoUri ? '' : required(env, 'DB_PASSWORD'),
        dbHost: mongoUri ? '' : required(env, 'DB_HOST'),
        dbName: required(env, 'DB_NAME'),
    };
}
