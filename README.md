# EventDBX Node Client (`eventdbxjs`)

`eventdbxjs` is a native addon (via [`napi-rs`](https://napi.rs/)) that gives the Node.js ecosystem first-class access to the EventDBX control socket. It wraps the Cap’n Proto control protocol exposed on port `6363`, delivering simple, Promise-based helpers for aggregate workflows.

> Status: experimental — the binding is still evolving alongside the EventDBX control protocol. Expect minor breaking changes until the API stabilises.

## Feature Highlights

- 🔌 Plug-and-play TCP client with optional token authentication.
- 🧾 JSON (de)serialisation for aggregates and event envelopes.
- 🧪 Built-in JSON Patch support (`[{ op, path, value }]`).
- 🧵 Async API surface designed for `async/await`.
- 🧱 Portable builds across macOS, Linux, and Windows via Cargo.

## Prerequisites

- Node.js 18 or newer (Node-API v8 compatible runtime).
- A Rust toolchain with `cargo` (install via [`rustup`](https://rustup.rs/)).
- `pnpm` 8+ or npm 9+ with [`corepack`](https://nodejs.org/api/corepack.html) enabled (`corepack enable`).

## Installing & Building

### Bootstrap this repository

```bash
corepack enable                       # once per machine
pnpm install                          # install JS dependencies and @napi-rs/cli
pnpm build                            # runs `napi build --platform --release`
```

`pnpm build` emits a platform-specific `eventdbx.*.node` binary in the project root.

If you prefer npm:

```bash
npm install
npm run build
```

### Using Cargo directly

Compile the addon from the repository root:

```bash
cargo build            # debug build
cargo build --release  # optimized build
```

The shared library is written to `target/{debug,release}` as `libeventdbx.*`. Rename it to `eventdbx.node` and place it beside your JavaScript entrypoint if you are wiring it up manually.

## Quick Start

```js
import { createClient } from 'eventdbxjs'

async function main() {
  const client = createClient({
    ip: process.env.EVENTDBX_HOST,
    port: Number(process.env.EVENTDBX_PORT) || 6363,
    token: process.env.EVENTDBX_TOKEN,
  })

  await client.connect()

  try {
    const aggregates = await client.list('person', { take: 20 })
    console.log(
      'known people:',
      aggregates.map((agg) => agg.aggregateId),
    )

    const snapshot = await client.createAggregate('person', 'p-110', 'person_registered', {
      payload: { name: 'Jane Doe', status: 'active' },
      note: 'seed aggregate',
    })
    console.log('created aggregate version', snapshot.version)

    await client.create('person', 'p-110', 'person_contact_added', {
      payload: { name: 'Jane Doe', status: 'active' },
      metadata: { note: 'seed data' },
      requireExisting: true,
    })

    await client.patch('person', 'p-110', 'person_status_updated', [
      { op: 'replace', path: '/status', value: 'inactive' },
    ])

    const history = await client.events('person', 'p-110')
    console.log('event count:', history.length)
  } finally {
    await client.disconnect()
  }
}

main().catch((err) => {
  console.error(err)
  process.exitCode = 1
})
```

## API Overview

| API                                                                         | Description                                                                                |
| --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `createClient(options?)`                                                    | Instantiate a client with optional `ip`, `port`, and `token` overrides.                    |
| `client.connect()` / `client.disconnect()`                                  | Open or close the TCP control socket.                                                      |
| `client.isConnected()`                                                      | Resolve to `true` when a socket is currently held.                                         |
| `client.endpoint`                                                           | Read-only `{ ip, port }` pulled from configuration.                                        |
| `client.list(aggregateType?, page?)`                                        | Fetch a page of aggregate snapshots, optionally filtered by type.                          |
| `client.get(aggregateType, aggregateId)`                                    | Resolve with the latest snapshot or `null` if none exists.                                 |
| `client.events(aggregateType, aggregateId, page?)`                          | Enumerate historical events for an aggregate.                                              |
| `client.create(aggregateType, aggregateId, eventType, options?)`            | Append an event with JSON payload/metadata and return the stored event.                    |
| `client.createAggregate(aggregateType, aggregateId, eventType, options?)`   | Create an aggregate with an initial event payload and return the resulting snapshot.       |
| `client.patch(aggregateType, aggregateId, eventType, operations, options?)` | Apply an RFC 6902 JSON Patch and return the updated aggregate snapshot.                    |
| `client.select(aggregateType, aggregateId, fields)`                         | Resolve with a JSON object containing only the requested fields when the aggregate exists. |

`PageOptions` supports `{ take, skip, includeArchived, archivedOnly }` for fine-grained pagination. Set `archivedOnly` to `true` to request archived aggregates exclusively—`includeArchived` is inferred when you do. When appending events, pass `requireExisting: true` if the aggregate must predate the write. `createAggregate` always requires an `eventType` and accepts optional `payload`, `metadata`, and `note` to seed the first snapshot.

## Runtime Configuration

The constructor falls back to environment variables when options are omitted:

| Variable         | Default     | Description                          |
| ---------------- | ----------- | ------------------------------------ |
| `EVENTDBX_HOST`  | `127.0.0.1` | Hostname or IP address of the socket |
| `EVENTDBX_PORT`  | `6363`      | Control-plane TCP port               |
| `EVENTDBX_TOKEN` | _empty_     | Authentication token sent on connect |

Passing explicit overrides is also supported:

```js
const client = createClient({
  ip: '10.1.0.42',
  port: 7000,
  token: 'super-secret',
})
await client.connect()
```

## TypeScript Surface

```ts
type Json = null | string | number | boolean | Json[] | { [key: string]: Json }

type JsonPatch =
  | { op: 'add' | 'replace' | 'test'; path: string; value: Json }
  | { op: 'remove'; path: string }
  | { op: 'move' | 'copy'; from: string; path: string }

interface ClientOptions {
  ip?: string
  port?: number
  token?: string
}

interface PageOptions {
  take?: number
  skip?: number
  includeArchived?: boolean
  archivedOnly?: boolean
}

interface AppendOptions {
  payload?: Json
  metadata?: Json
  note?: string | null
  token?: string
  requireExisting?: boolean
}

interface CreateAggregateOptions {
  token?: string
  payload?: Json
  metadata?: Json
  note?: string | null
}

interface PatchOptions {
  metadata?: Json
  note?: string | null
  token?: string
}

interface ClientEndpoint {
  ip: string
  port: number
}

interface Aggregate<TState = Json> {
  aggregateType: string
  aggregateId: string
  version: number
  state: TState
  merkleRoot: string
  archived: boolean
}

interface Event<TPayload = Json> {
  aggregateType: string
  aggregateId: string
  eventType: string
  version: number
  sequence: number
  payload: TPayload
  metadata: {
    eventId: string
    createdAt: string
    issuedBy?: { group?: string; user?: string }
    note?: string | null
  }
  hash: string
  merkleRoot: string
}

class DbxClient {
  constructor(options?: ClientOptions)
  connect(): Promise<void>
  disconnect(): Promise<void>
  isConnected(): Promise<boolean>
  readonly endpoint: ClientEndpoint

  list<TState = Json>(aggregateType?: string, opts?: PageOptions): Promise<Aggregate<TState>[]>
  get<TState = Json>(aggregateType: string, aggregateId: string): Promise<Aggregate<TState> | null>
  events<TPayload = Json>(aggregateType: string, aggregateId: string, opts?: PageOptions): Promise<Event<TPayload>[]>
  create<TPayload = Json>(
    aggregateType: string,
    aggregateId: string,
    eventType: string,
    opts?: AppendOptions,
  ): Promise<Event<TPayload>>
  createAggregate<TState = Json>(
    aggregateType: string,
    aggregateId: string,
    eventType: string,
    opts?: CreateAggregateOptions,
  ): Promise<Aggregate<TState>>
  patch<TState = Json>(
    aggregateType: string,
    aggregateId: string,
    eventType: string,
    operations: JsonPatch[],
    opts?: PatchOptions,
  ): Promise<Aggregate<TState>>
  select(aggregateType: string, aggregateId: string, fields: string[]): Promise<Json | null>
}

declare function createClient(options?: ClientOptions): DbxClient
```

All methods return Promises and throw regular JavaScript `Error` instances on failure (network issues, protocol validation errors, rejected patch operations, etc.) — wrap awaited calls in `try/catch`.

## Testing & Tooling

- `pnpm test` runs the JavaScript test suite (AVA).
- `cargo test` runs the Rust unit tests for the binding and shared client.
- `pnpm run lint` executes `oxlint` plus project formatting tasks.
- `pnpm run bench` runs micro benchmarks against an attached EventDBX instance.

## Development Notes

- The addon talks Cap’n Proto over TCP via the shared `ControlClient` in `src/plugin_api`.
- JSON Patch payloads **must** be valid RFC 6902 arrays; malformed operations are rejected by the server.
- WASI/worker bindings are generated under `eventdbx.wasi.*` for experimentation with Node WASI and browser runtimes.

### Tests

- `pnpm test` automatically exercises the JS bindings. If `EVENTDBX_TEST_PORT` is reachable, the suite runs a live smoke test; otherwise it falls back to mocked responses to keep coverage offline-friendly.
- `cargo test` runs the Rust unit tests that back the Node bindings.

## Roadmap

- [x] Add `package.json` metadata and prebuild scripts for automated npm releases.
- [x] Regenerate the published TypeScript definitions from the napi-rs metadata.
- [x] Surface strongly typed helpers for common aggregate payloads.
- [ ] Support streaming subscriptions once EventDBX exposes them on the control socket.
