import * as crypto from 'crypto';
import * as jwt from 'jsonwebtoken';

type AccessClaims = {
    id?: string;
    user_id?: string;
    tokenType?: string;
    typ?: string;
};

function unwrap(raw: string): string {
    const trimmed = raw.trim();
    if (
        (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
        (trimmed.startsWith('"') && trimmed.endsWith('"'))
    ) {
        return trimmed.slice(1, -1);
    }
    return trimmed;
}

function publicKeyFromEnv(raw: string): crypto.KeyObject {
    const value = unwrap(raw).replace(/\\n/g, '\n');
    if (value.startsWith('{')) {
        const parsed = JSON.parse(value.replace(/\\"/g, '"')) as crypto.JsonWebKey;
        return crypto.createPublicKey({ key: parsed, format: 'jwk' });
    }
    return crypto.createPublicKey(value);
}

export class AccessVerifier {
    private readonly key: crypto.KeyObject;

    constructor(publicKey: string) {
        this.key = publicKeyFromEnv(publicKey);
    }

    userId(token: string): string | null {
        try {
            const decoded = jwt.verify(token, this.key, {
                algorithms: ['RS256'],
            }) as AccessClaims;
            if (decoded.tokenType === 'refresh' || decoded.typ === 'refresh') {
                return null;
            }
            const id = decoded.id || decoded.user_id;
            if (!id) return null;
            return String(id);
        } catch {
            return null;
        }
    }
}
