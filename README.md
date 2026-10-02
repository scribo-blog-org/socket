# Scribo socket

WebSocket-процесс для сообщений и присутствия. HTTP API он не заменяет и сообщения в базу не пишет. Новое сообщение создаёт backend, публикует событие в Redis, а этот процесс доставляет его тем сокетам, которые подписаны на комнату.

Прод: `wss://scribo-blog.duckdns.org/ws`. Снаружи отдельного порта нет. Nginx проксирует путь `/ws` на контейнер `socket:3002` и держит соединение с заголовками `Upgrade`.

## Место в системе

```
браузер  --WSS /ws-->  nginx  -->  этот процесс :3002
                                     ├─ проверка access JWT, только публичный ключ RS256
                                     ├─ MongoDB Atlas: участник беседы или нет
                                     └─ Redis: подписка на scribo:events и присутствие
```

Backend публикует в канал `scribo:events` JSON `{ room, event, payload }`. Комнаты: `user:<id>` и `chat:<id>`. Сокет, который в этой комнате, получает событие.

Присутствие хранится в Redis и рассылается по отдельному каналу. После пересоздания Redis онлайн пропадает: у Redis в compose нет снимков и AOF.

При старте одна строка: порт, Redis, хост и имя Mongo. Ошибки соединения пишутся в stderr. Журнала каждого кадра нет.

## Что принимает сокет

Клиент шлёт JSON.

| Тип | Смысл |
| --- | --- |
| `auth` | Access JWT. Без него подписка на комнаты не проходит |
| `subscribe` | Войти в `user:<id>` или `chat:<id>`. Для чата пользователь должен быть участником |
| `unsubscribe` | Выйти из комнаты |
| `presence:query` | Спросить, кто из переданных id сейчас онлайн |

Чужой чат не открывается: перед `subscribe` на `chat:<id>` процесс читает документ беседы в Mongo и сверяет `participants`. Других коллекций он не меняет.

`GET /health` на этом же порту отвечает `{ "ok": true }`. Это проверка контейнера, не публичный `/health` сайта. Публичный `/health` обслуживает backend.

## Чего здесь нет

Нет закрытого ключа и нет секрета refresh. Если в окружение попали `JWT_PRIVATE_KEY` или `JWT_REFRESH_KEY`, процесс завершается при старте. Подписывать токены он не должен.

Нет записи сообщений, лайков, постов и почты. Это backend.

## Локальный запуск

Node.js 22. Рядом должны быть Redis и та же Mongo, что у API. Публичный ключ — тот же PEM, что `JWT_PUBLIC_KEY` у backend.

```bash
npm install
npm run build
npm start
```

Слушает `0.0.0.0` и `PORT`, по умолчанию `3002`.

| Переменная | Смысл |
| --- | --- |
| `PORT` | Порт, по умолчанию `3002` |
| `REDIS_URL` | С хоста `redis://127.0.0.1:6379`. В compose `redis://redis:6379` |
| `JWT_PUBLIC_KEY` | Публичный ключ RS256, PEM |
| `MONGODB_URI` | Необязательно. Полная строка, например локальная Mongo из `infra/local/compose.yml`; тогда `DB_USER`, `DB_PASSWORD`, `DB_HOST` не нужны, `DB_NAME` остаётся обязательным |
| `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME` | Доступ к Mongo. `DB_HOST` — хост кластера, без схемы и без учётных данных |

## Как устроен код

```
src/
  index.ts      загрузка конфига, Mongo, старт, SIGTERM
  config.ts     env, запрет закрытых ключей
  access.ts     проверка access JWT
  mongo.ts      один запрос: участник беседы
  server.ts     HTTP /health, WebSocket, Redis
  presence.ts   набор онлайн-id в Redis
```

Зависимости узкие: `ws`, `ioredis`, официальный драйвер MongoDB, `jsonwebtoken`. Nest здесь нет.

## Скрипты

```bash
npm run build
npm start
npm run lint
npm run test
```

`lint` и `test` — заглушки с кодом 0. Отдельного линтера и набора тестов в репозитории нет. Проверка pull request из-за этого не падает.

## Выкладка

Push в `master` собирает образ `ghcr.io/scribo-blog-org/socket` и поднимает сервис `socket` в compose. Pull request в `master` гоняет lint, test и `docker build` без публикации. Машина, nginx и Redis описаны в репозитории `infra`.
