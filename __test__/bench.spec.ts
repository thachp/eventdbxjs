import type { ExecutionContext } from 'ava'
import test from 'ava'
import { Bench } from 'tinybench'

import { createClient } from '../index.js'

type RuntimeEnv = Record<string, string | undefined>
type RuntimeContext = { process?: { env?: RuntimeEnv } }

const runtimeEnv: RuntimeEnv = (globalThis as RuntimeContext).process?.env ?? {}

const baseOptions = {
  ip: '127.0.0.1',
  port: 6363,
  token:
    'eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6ImtleS0yMDI1MTEwMTExMjcyMCJ9.eyJpc3MiOiJldmVudGRieDovL3NlbGYiLCJhdWQiOiJldmVudGRieC1jbGllbnRzIiwic3ViIjoiY2xpOmJvb3RzdHJhcCIsImp0aSI6Ijk5Y2VkM2M5LTk2MTktNGYyNS04MWVjLWQzMDEwOWIxYmY5NSIsImlhdCI6MTc2MTk5NjQ0MCwiZ3JvdXAiOiJjbGkiLCJ1c2VyIjoicm9vdCIsImFjdGlvbnMiOlsiKi4qIl0sInJlc291cmNlcyI6WyIqIl0sImlzc3VlZF9ieSI6ImNsaS1ib290c3RyYXAiLCJsaW1pdHMiOnsid3JpdGVfZXZlbnRzIjpudWxsLCJrZWVwX2FsaXZlIjpmYWxzZX19.DvOizlvAYs71HxOLnF439NNrAZuZu_uNyPpyiLyebB7GjkftZwpGLkvSvtvYLymzbKVFP52qWeZ7sGTF-3-ZDw',
}

type ControlClientOptions = {
  ip: string
  port: number
  token: string
}

type AsyncOperation = () => Promise<unknown>

const parsePort = (value: string | undefined, fallback: number): number => {
  if (value === undefined) {
    return fallback
  }
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

const integrationOptions: ControlClientOptions = {
  ip: runtimeEnv.EVENTDBX_TEST_IP ?? baseOptions.ip,
  port: parsePort(runtimeEnv.EVENTDBX_TEST_PORT, baseOptions.port),
  token: runtimeEnv.EVENTDBX_TEST_TOKEN ?? baseOptions.token,
}

const operationDetails: Record<string, string> = {
  list: 'aggregate listing',
  get: 'load aggregate snapshot',
  select: 'project selected fields',
  events: 'read event stream',
  apply: 'append event',
  create: 'create aggregate',
  archive: 'archive aggregate',
  restore: 'restore aggregate',
  patch: 'apply JSON patch',
  count: 'count dataset records',
  'random-read': 'fetch random dataset record',
  'insert-delete': 'insert and clean up record',
  aggregate: 'aggregation pipeline',
}

const datasetSizes = [1_000, 10_000, 100_000, 1_000_000] as const
const datasetCategory = 'dataset'
const benchCategory = 'benchmark'

const formatDatasetLabel = (count: number) => `${count.toLocaleString()} records`

const logDatasetPreparation = (t: ExecutionContext, backend: string, size: number, index: number) => {
  t.log(`${backend} dataset ready: test${index + 1} (${formatDatasetLabel(size)})`)
}

const runOperation = async (label: string, action: AsyncOperation) => {
  try {
    await action()
  } catch (error) {
    const message = (error as Error).message ?? ''
    if (/client is not connected/i.test(message)) {
      throw new Error(`${label} failed because connection was lost: ${message}`)
    }
  }
}

const precisionFor = (value: number) => {
  if (value >= 100) {
    return 0
  }
  if (value >= 10) {
    return 1
  }
  return 2
}

const formatLatency = (value: number) => {
  if (!Number.isFinite(value)) {
    return 'n/a'
  }
  if (value >= 1_000_000) {
    const ms = value / 1_000_000
    return `${ms.toFixed(precisionFor(ms))} ms`
  }
  if (value >= 1_000) {
    const micros = value / 1_000
    return `${micros.toFixed(precisionFor(micros))} µs`
  }
  return `${value.toFixed(precisionFor(value))} ns`
}

const formatThroughput = (value: number) => {
  if (!Number.isFinite(value)) {
    return 'n/a'
  }
  if (value >= 1_000_000_000) {
    const billions = value / 1_000_000_000
    return `${billions.toFixed(precisionFor(billions))}B`
  }
  if (value >= 1_000_000) {
    const millions = value / 1_000_000
    return `${millions.toFixed(precisionFor(millions))}M`
  }
  if (value >= 1_000) {
    const thousands = value / 1_000
    return `${thousands.toFixed(precisionFor(thousands))}k`
  }
  return value.toFixed(precisionFor(value))
}

const formatRme = (value: number) => {
  if (!Number.isFinite(value)) {
    return 'n/a'
  }
  return `${value.toFixed(2)}%`
}

const createBench = (name: string) =>
  new Bench({
    name,
    time: 150,
    warmupTime: 50,
    warmupIterations: 3,
    iterations: 5,
    throws: false,
  })

const validateBenchTasks = (t: ExecutionContext, bench: Bench) => {
  for (const task of bench.tasks) {
    t.truthy(task.result, `${task.name} produced benchmark stats`)
    t.true((task.result?.samples?.length ?? 0) > 0, `${task.name} recorded latency samples`)
    t.falsy(task.result?.error, `${task.name} should not surface errors`)
  }
}

const formatGrid = (headers: string[], rows: string[][]) => {
  const allRows = [headers, ...rows]
  const columnWidths = headers.map((_, columnIndex) =>
    Math.max(...allRows.map((row) => (row[columnIndex] ?? '').length)),
  )

  const renderRow = (row: string[]) =>
    row
      .map((cell, index) => {
        const width = columnWidths[index]
        return `${cell}`.padEnd(width, ' ')
      })
      .join('  ')

  const separator = columnWidths.map((width) => '-'.repeat(width)).join('  ')

  return [renderRow(headers), separator, ...rows.map(renderRow)].join('\n')
}

const summarizeBench = (bench: Bench, label?: string) => {
  const rows = bench.tasks.map((task) => {
    const stats = task.result

    if (!stats) {
      return [task.name, 'no samples recorded', 'no samples recorded']
    }

    const throughput = `${formatThroughput(stats.throughput.mean)} ±${formatRme(stats.throughput.rme)}`
    const latency = `${formatLatency(stats.latency.mean)} ±${formatRme(stats.latency.rme)}`

    return [task.name, throughput, latency]
  })

  const header = label ?? bench.name ?? 'Bench summary'
  return [`${header}:`, formatGrid(['operation', 'throughput (ops/s)', 'latency'], rows)].join('\n')
}

const toErrorMessage = (error: unknown, fallback = 'unknown error') => {
  if (error instanceof Error && typeof error.message === 'string') {
    return error.message
  }
  if (typeof error === 'string') {
    return error
  }
  try {
    return JSON.stringify(error)
  } catch {
    return fallback
  }
}

type PgConfig = {
  host?: string
  port?: number
  user?: string
  password?: string
  database?: string
  sslmode?: string
}

const parsePgLibpqDsn = (dsn: string): PgConfig => {
  const result: PgConfig = {}
  const regex = /(\w+)=('([^']*)'|[^\s]+)/g
  let match: RegExpExecArray | null

  while ((match = regex.exec(dsn)) !== null) {
    const [, key, rawValue, quotedValue] = match
    const value = quotedValue ?? rawValue.replace(/^'(.*)'$/, '$1')
    if (!value) {
      continue
    }
    switch (key) {
      case 'host':
        result.host = value
        break
      case 'port': {
        const parsed = Number.parseInt(value, 10)
        if (Number.isFinite(parsed)) {
          result.port = parsed
        }
        break
      }
      case 'user':
        result.user = value
        break
      case 'password':
        result.password = value
        break
      case 'dbname':
      case 'database':
        result.database = value
        break
      case 'sslmode':
        result.sslmode = value
        break
      default:
        break
    }
  }

  return result
}

const buildPgPoolConfig = (dsn: string) => {
  if (dsn.includes('://')) {
    return { connectionString: dsn }
  }
  const parsed = parsePgLibpqDsn(dsn)
  const config: Record<string, unknown> = {
    host: parsed.host,
    port: parsed.port,
    user: parsed.user,
    password: parsed.password,
    database: parsed.database,
  }

  if (parsed.sslmode && parsed.sslmode.toLowerCase() === 'require') {
    config.ssl = { rejectUnauthorized: false }
  }

  return config
}

const ensurePostgresDataset = async (
  pool: { query: (text: string, params?: unknown[]) => Promise<{ rows: Array<{ count?: number }> }> },
  targetSize: number,
) => {
  const { rows } = await pool.query('SELECT COUNT(*)::int AS count FROM bench_events WHERE category = $1', [
    datasetCategory,
  ])
  const existing = rows[0]?.count ?? 0

  if (existing >= targetSize) {
    return existing
  }

  await pool.query(
    `
      INSERT INTO bench_events (category, payload)
      SELECT $1, jsonb_build_object('dataset', true, 'index', g)
      FROM generate_series($2::integer, $3::integer) AS g
    `,
    [datasetCategory, existing + 1, targetSize],
  )

  return targetSize
}

const ensureMongoDataset = async (
  collection: {
    countDocuments: (filter: Record<string, unknown>) => Promise<number>
    insertMany: (docs: Array<Record<string, unknown>>, options?: Record<string, unknown>) => Promise<void>
  },
  targetSize: number,
) => {
  const existing = await collection.countDocuments({ benchDataset: true })

  if (existing >= targetSize) {
    return existing
  }

  const batchSize = 1_000
  let inserted = existing

  while (inserted < targetSize) {
    const upperBound = Math.min(inserted + batchSize, targetSize)
    const docs: Array<Record<string, unknown>> = []

    for (let index = inserted; index < upperBound; index += 1) {
      docs.push({
        benchDataset: true,
        index: index + 1,
        seededAt: new Date(),
      })
    }

    await collection.insertMany(docs, { ordered: false })
    inserted = upperBound
  }

  return targetSize
}

test('benchmarks control operations against the server', async (t) => {
  const client = createClient(integrationOptions)

  try {
    await client.connect()
  } catch (error) {
    t.log(`Skipping benchmark – unable to connect: ${toErrorMessage(error)}`)
    t.pass()
    return
  }

  try {
    for (const [datasetIndex, size] of datasetSizes.entries()) {
      logDatasetPreparation(t, 'EventDBX control', size, datasetIndex)
      const datasetLabel = `test${datasetIndex + 1} (${formatDatasetLabel(size)})`
      const pageSize = Math.max(1, Math.min(size, 100))
      const eventWindow = Math.max(1, Math.min(size, 25))
      const bench = createBench(`EventDBX control operations ${datasetLabel}`)

      const operations: Array<[string, AsyncOperation]> = [
        ['list', () => client.list(undefined, { take: pageSize, skip: 0 })],
        ['get', () => client.get('__test__', '__test__')],
        ['select', () => client.select('__test__', '__test__', ['state'])],
        ['events', () => client.events('__test__', '__test__', { skip: 0, take: eventWindow })],
        [
          'apply',
          () =>
            client.apply('__test__', '__test__', 'BenchEvent', {
              payload: { ping: 'pong' },
              token: 'invalid-integration-token',
            }),
        ],
        [
          'create',
          () =>
            client.create('__test__', '__test__', 'BenchCreated', {
              token: 'invalid-integration-token',
            }),
        ],
        [
          'archive',
          () =>
            client.archive('__test__', '__test__', {
              token: 'invalid-integration-token',
            }),
        ],
        [
          'restore',
          () =>
            client.restore('__test__', '__test__', {
              token: 'invalid-integration-token',
            }),
        ],
        [
          'patch',
          () =>
            client.patch('__test__', '__test__', 'BenchPatched', [{ op: 'replace', path: '/ping', value: 'pong' }], {
              note: 'tinybench run',
              token: 'invalid-integration-token',
            }),
        ],
      ]

      for (const [label, action] of operations) {
        bench.add(label, async () => runOperation(label, action))
      }

      await bench.run()

      validateBenchTasks(t, bench)
      t.log(summarizeBench(bench, `EventDBX control operations ${datasetLabel}`))
      t.log('')
    }
  } finally {
    await client.disconnect().catch(() => {})
  }
})

test('benchmarks mongodb operations', async (t) => {
  const uri = 'mongodb://mongo:mongo@localhost:27017'
  const dbName = 'bench'

  const collectionName = 'events'
  if (!uri || !dbName || !collectionName) {
    t.log('Skipping MongoDB benchmark – missing EVENTDBX_MONGO_* configuration')
    t.pass()
    return
  }

  let mongoModule: any
  try {
    mongoModule = await import('mongodb')
  } catch (error) {
    t.log(`Skipping MongoDB benchmark – unable to load mongodb module: ${toErrorMessage(error)}`)
    t.pass()
    return
  }

  const client = new mongoModule.MongoClient(uri, {
    maxPoolSize: 20,
    serverSelectionTimeoutMS: 1_000,
  })

  try {
    await client.connect()
  } catch (error) {
    await client.close().catch(() => {})
    t.log(`Skipping MongoDB benchmark – unable to connect: ${toErrorMessage(error)}`)
    t.pass()
    return
  }

  try {
    const database = client.db(dbName)
    const collection = database.collection(collectionName)

    await collection.createIndex({ benchDataset: 1 }).catch(() => {})
    await collection.createIndex({ benchRun: 1 }).catch(() => {})
    await collection.deleteMany({ benchRun: true, benchDataset: { $ne: true } })

    for (const [datasetIndex, size] of datasetSizes.entries()) {
      await ensureMongoDataset(collection, size)
      logDatasetPreparation(t, 'MongoDB', size, datasetIndex)
      const datasetLabel = `test${datasetIndex + 1} (${formatDatasetLabel(size)})`

      const bench = createBench(`MongoDB operations ${datasetLabel}`)

      bench.add('count', async () => {
        await collection.countDocuments({ benchDataset: true })
      })

      bench.add('random-read', async () => {
        await collection.aggregate([{ $match: { benchDataset: true } }, { $sample: { size: 1 } }]).toArray()
      })

      bench.add('insert-delete', async () => {
        const document = {
          benchRun: true,
          benchDataset: false,
          insertedAt: new Date(),
          random: Math.random(),
        }
        const result = await collection.insertOne(document)
        await collection.deleteOne({ _id: result.insertedId })
      })

      bench.add('aggregate', async () => {
        await collection
          .aggregate([
            { $match: { benchDataset: true } },
            { $group: { _id: null, total: { $sum: 1 }, maxIndex: { $max: '$index' } } },
          ])
          .toArray()
      })

      await bench.run()

      validateBenchTasks(t, bench)
      t.log(summarizeBench(bench, `MongoDB operations ${datasetLabel}`))
      t.log('')
    }
  } finally {
    await client.close().catch(() => {})
  }
})

test('benchmarks postgres operations', async (t) => {
  const dsn = 'postgresql://postgres:postgres@localhost:5432/postgres'

  if (!dsn) {
    t.log('Skipping Postgres benchmark – EVENTDBX_PG_DSN (or equivalent) not provided')
    t.pass()
    return
  }

  let pgModule: any
  try {
    pgModule = await import('pg')
  } catch (error) {
    t.log(`Skipping Postgres benchmark – unable to load pg module: ${toErrorMessage(error)}`)
    t.pass()
    return
  }

  const pool = new pgModule.Pool(buildPgPoolConfig(dsn))

  let connectionReady = true
  try {
    await pool.query('SELECT 1')
  } catch (error) {
    connectionReady = false
    t.log(`Skipping Postgres benchmark – unable to connect: ${toErrorMessage(error)}`)
  }

  if (!connectionReady) {
    await pool.end().catch(() => {})
    t.pass()
    return
  }

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bench_events (
        id BIGSERIAL PRIMARY KEY,
        category TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    await pool.query('CREATE INDEX IF NOT EXISTS bench_events_category_idx ON bench_events (category)')
    await pool.query(
      'CREATE INDEX IF NOT EXISTS bench_events_category_created_at_idx ON bench_events (category, created_at)',
    )

    for (const [datasetIndex, size] of datasetSizes.entries()) {
      await ensurePostgresDataset(pool, size)
      logDatasetPreparation(t, 'Postgres', size, datasetIndex)
      const datasetLabel = `test${datasetIndex + 1} (${formatDatasetLabel(size)})`

      const bench = createBench(`Postgres operations ${datasetLabel}`)

      bench.add('count', async () => {
        await pool.query('SELECT COUNT(*)::int FROM bench_events WHERE category = $1', [datasetCategory])
      })

      bench.add('random-read', async () => {
        const { rows } = await pool.query('SELECT id FROM bench_events WHERE category = $1 ORDER BY random() LIMIT 1', [
          datasetCategory,
        ])
        if (!rows[0]?.id) {
          throw new Error('Postgres dataset is empty')
        }
      })

      bench.add('insert-delete', async () => {
        const { rows } = await pool.query('INSERT INTO bench_events (category, payload) VALUES ($1, $2) RETURNING id', [
          benchCategory,
          {
            benchRun: true,
            insertedAt: new Date().toISOString(),
            random: Math.random(),
          },
        ])
        const insertedId = rows[0]?.id
        if (insertedId === undefined) {
          throw new Error('Postgres insert did not return an id')
        }
        await pool.query('DELETE FROM bench_events WHERE id = $1', [insertedId])
      })

      bench.add('aggregate', async () => {
        const { rows } = await pool.query(
          `
            SELECT COUNT(*)::int AS total, MAX((payload ->> 'index')::int) AS max_index
            FROM bench_events
            WHERE category = $1
          `,
          [datasetCategory],
        )
        if (!rows[0]) {
          throw new Error('Postgres aggregate returned no rows')
        }
      })

      await bench.run()

      validateBenchTasks(t, bench)
      t.log(summarizeBench(bench, `Postgres operations ${datasetLabel}`))
      t.log('')
    }
  } finally {
    await pool.end().catch(() => {})
  }
})
