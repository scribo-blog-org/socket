# Scribo socket

Realtime delivery for the Scribo blog: chat messages, typing indicators and presence. This process is not a second API. It never writes a message, a post or a profile. The backend owns all state; this service only pushes events to the sockets that are entitled to see them.

Production: `wss://scribo.pp.ua/ws`. Staging: `wss://scribo-stage.pp.ua/ws`. No port is published to the internet. The nginx container in the `edge` stack proxies the `/ws` path to `<stack>-socket:3002` and keeps the connection open with the `Upgrade` headers.

## Where it sits

```
browser  --WSS /ws-->  nginx  -->  this process :3002
                                   |- access JWT check, RS256 public key only
                                   |- MongoDB: conversation membership
                                   `- Redis: scribo:events, presence, typing
```

The backend publishes `{ room, event, payload }` as JSON into the Redis channel `scribo:events`. Rooms are `user:<id>` and `chat:<id>`. Every socket subscribed to that room receives the event. Presence and typing use their own channels and their own Redis keys.

Nothing here survives a Redis restart: the Redis instance in compose runs without RDB snapshots and without AOF. After the container is recreated, presence and typing start empty. That is expected, both are ephemeral by nature.

Startup prints a single line with the port, the Redis target and the Mongo host and database name. Connection failures go to stderr. Individual frames are not logged.

## Repositories

| Repository | Role |
| --- | --- |
| `frontend` | Next.js client; opens the socket and subscribes to rooms |
| `backend` | NestJS HTTP API; owns the data and publishes the events |
| `socket` | this repository |
| `infra` | compose files, nginx, certificates, server scripts |

## How it talks to the rest of the system

The browser authenticates with the same access JWT it uses for the HTTP API. This process verifies it with the RS256 **public** key and nothing else. It cannot issue tokens and it has no refresh secret.

Sending a message is still an HTTP call to the backend. The backend writes it to Mongo, then publishes an event; this service fans it out. The client therefore never has to choose between "did the write succeed" and "did the socket deliver it" — the write is the HTTP response, the socket is only the notification.

Membership is checked here, not trusted from the client. Before a socket may join `chat:<id>`, the process reads the conversation document from the same MongoDB the backend uses and verifies that the user is in `participants`. It performs no writes other than refreshing the user's last-activity timestamp.

## Protocol

The client sends JSON control frames.

| Type | Meaning |
| --- | --- |
| `auth` | Access JWT. Until it succeeds, no room may be joined |
| `subscribe` | Join `user:<id>` or `chat:<id>`. Chat rooms require membership |
| `unsubscribe` | Leave a room |
| `presence:query` | Ask which of the given user ids are online right now |
| `typing` | Report that the user is typing in a conversation, or stopped |
| `typing:query` | Ask who is currently typing in the conversations this user takes part in |

Typing is rate limited on the server, not only in the client. A pulse refreshes a short-lived Redis key (`TYPING_TTL_MS`, two seconds) and is fanned out only to the other participants of that conversation; a gate key drops pulses that arrive faster than the allowed rate. When the key expires, or the socket disconnects, a stop event is published automatically, so a client that closes the tab mid-word does not leave a stuck indicator.

Presence works the same way: a set of online connection ids per user, refreshed every 15 seconds while the socket is open, with an activity timestamp written back to Mongo at most once a minute.

`GET /health` on the same port returns `{ "ok": true }`. This is the container healthcheck, not the public `/health` of the site — that one is served by the backend.

## What is deliberately absent

There is no private key and no refresh secret. If `JWT_PRIVATE_KEY` or `JWT_REFRESH_KEY` appear in the environment, the process exits during startup rather than running with credentials it must never hold.

There is no message persistence, no mail, no business logic. All of that belongs to the backend.

## Running locally

Node.js 22. A Redis instance and the same MongoDB the API uses must be reachable. The public key must be the exact PEM that the backend has in `JWT_PUBLIC_KEY`.

```bash
npm install
npm run build
npm start
```

Listens on `0.0.0.0` and `PORT`, `3002` by default.

| Variable | Meaning |
| --- | --- |
| `PORT` | Listen port, `3002` by default |
| `REDIS_URL` | `redis://127.0.0.1:6379` from the host, `redis://redis:6379` inside compose |
| `JWT_PUBLIC_KEY` | RS256 public key, PEM |
| `MONGODB_URI` | Optional full connection string. When set, `DB_USER`, `DB_PASSWORD` and `DB_HOST` are ignored, but `DB_NAME` is still required |
| `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME` | Mongo access. `DB_HOST` is the cluster host only, without scheme or credentials |

The quickest local setup is `infra/local`, which brings up Mongo and Redis next to the three applications.

## Layout

```
src/
  index.ts      config load, Mongo connection, startup, SIGTERM
  config.ts     environment parsing, refusal of private keys
  access.ts     access JWT verification
  mongo.ts      membership lookup, participant lists, last-activity touch
  server.ts     HTTP /health, WebSocket server, Redis subscriptions
  presence.ts   online set in Redis
  typing.ts     typing keys, rate gate and fan-out lists in Redis
```

Dependencies are deliberately narrow: `ws`, `ioredis`, the official MongoDB driver and `jsonwebtoken`. There is no Nest here, and no framework at all.

## Scripts

```bash
npm run build
npm start
npm run lint
npm run test
```

`lint` and `test` are placeholders that exit 0. The repository has no linter configuration and no test suite of its own, and the pull request checks are written so that this does not fail the pipeline.

## Deployment

A push to `master` builds `ghcr.io/scribo-blog-org/socket` on an ARM runner, publishes the `latest` and commit-sha tags, and then pulls and restarts the `socket` service of the `prod` stack over SSH. A push to `dev` does the same with the `staging` tag against the `stage` stack. Pull requests run lint, test and a local `docker build` without publishing.

The machine, nginx, Redis and the compose layout are documented in the `infra` repository.
