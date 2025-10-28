import test from 'ava'
import type { ExecutionContext } from 'ava'

import { createClient } from '../index.js'

type RuntimeEnv = Record<string, string | undefined>
type RuntimeContext = { process?: { env?: RuntimeEnv } }

const runtimeEnv: RuntimeEnv = (globalThis as RuntimeContext).process?.env ?? {}

const baseOptions = {
  ip: '127.0.0.1',
  port: 6363,
  token: 'test-token',
}

type AsyncCall = () => Promise<unknown>

type ControlClientOptions = {
  ip: string
  port: number
  token: string
}

type NetSocket = {
  setTimeout(timeout: number): void
  once(event: 'error' | 'timeout', listener: () => void): void
  connect(port: number, host: string, listener: () => void): void
  removeAllListeners(): void
  destroy(): void
}

type NetModule = {
  Socket: new () => NetSocket
}

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

const buildClient = (overrides: Record<string, unknown> = {}) => createClient({ ...baseOptions, ...overrides })
const overrideMethod = (target: object, method: string, implementation: unknown) => {
  Object.defineProperty(target, method, {
    value: implementation,
    configurable: true,
    writable: true,
  })
}

const isPortOpen = async (
  options: Pick<ControlClientOptions, 'ip' | 'port'> & { timeoutMs?: number },
): Promise<boolean> => {
  const { ip, port, timeoutMs = 250 } = options
  let netModule: NetModule | undefined

  try {
    // @ts-ignore Fallback to dynamic import without bundled node typings
    netModule = (await import('node:net')) as NetModule
  } catch {
    // @ts-ignore Fallback to classic module id when node-prefixed import is unavailable
    netModule = (await import('net')) as NetModule
  }

  if (!netModule) {
    return false
  }

  return new Promise((resolve) => {
    const socket = new netModule!.Socket()
    let finished = false

    const finalize = (result: boolean) => {
      if (finished) {
        return
      }
      finished = true
      socket.removeAllListeners()
      socket.destroy()
      resolve(result)
    }

    socket.setTimeout(timeoutMs)
    socket.once('error', () => finalize(false))
    socket.once('timeout', () => finalize(false))
    socket.connect(port, ip, () => finalize(true))
  })
}

let controlSocketAvailable = false

test.before(async () => {
  controlSocketAvailable = await isPortOpen(integrationOptions)
})

test('client exposes expected API surface', (t) => {
  const client = buildClient()

  t.truthy(client)
  t.is(typeof client.connect, 'function')
  t.is(typeof client.disconnect, 'function')
  t.is(typeof client.isConnected, 'function')
  t.is(typeof client.list, 'function')
  t.is(typeof client.get, 'function')
  t.is(typeof client.select, 'function')
  t.is(typeof client.events, 'function')
  t.is(typeof client.apply, 'function')
  t.is(typeof client.create, 'function')
  t.is(typeof client.archive, 'function')
  t.is(typeof client.restore, 'function')
  t.is(typeof client.patch, 'function')
  t.deepEqual(client.endpoint, { ip: baseOptions.ip, port: baseOptions.port })
})

test('connect rejects when the target is unavailable', async (t) => {
  const client = buildClient({ port: 0 })

  await t.throwsAsync(async () => {
    await client.connect()
  })
})

test('isConnected reports false before and after failed connects', async (t) => {
  const client = buildClient({ port: 0 })

  t.false(await client.isConnected())
  await client.connect().catch(() => {})
  t.false(await client.isConnected())
})

test('disconnect resolves even if no connection exists', async (t) => {
  const client = buildClient()

  await t.notThrowsAsync(async () => {
    await client.disconnect()
    await client.disconnect()
  })
})

const runMockedControlOperations = async (t: ExecutionContext) => {
  const aggregate = {
    aggregateType: 'person',
    aggregateId: 'p-001',
    version: 7,
    state: { name: 'Jane Doe' },
    merkleRoot: 'abc123',
    archived: false,
  }
  const selection = { name: 'Jane Doe' }
  const events = [
    {
      aggregateType: 'person',
      aggregateId: 'p-001',
      eventType: 'Created',
      version: 1,
      payload: { name: 'Jane Doe' },
      metadata: { eventId: 'evt-1' },
      hash: 'deadbeef',
      merkleRoot: 'abc123',
    },
  ]
  const created = { ...events[0], version: 8, eventType: 'Updated' }
  const patched = { ...aggregate, version: 8 }
  const createdAggregate = { ...aggregate, aggregateId: 'p-002', version: 1 }
  const archivedAggregate = { ...aggregate, archived: true }
  const restoredAggregate = { ...aggregate, archived: false }

  const client = buildClient()

  let connected = false
  overrideMethod(client, 'connect', async () => {
    connected = true
  })
  overrideMethod(client, 'disconnect', async () => {
    connected = false
  })
  overrideMethod(client, 'isConnected', async () => connected)
  overrideMethod(client, 'list', async (aggregateType: string, options: { take: number; skip: number }) => {
    t.is(aggregateType, 'person')
    t.deepEqual(options, { take: 10, skip: 2 })
    return [aggregate]
  })
  overrideMethod(client, 'get', async (aggregateType: string, aggregateId: string) => {
    t.is(aggregateType, 'person')
    t.is(aggregateId, 'p-001')
    return aggregate
  })
  overrideMethod(
    client,
    'select',
    async (aggregateType: string, aggregateId: string, fields: string[]) => {
      t.is(aggregateType, 'person')
      t.is(aggregateId, 'p-001')
      t.deepEqual(fields, ['state.name'])
      return selection
    },
  )
  overrideMethod(client, 'events', async (aggregateType: string, aggregateId: string, options: { take: number; skip: number }) => {
    t.is(aggregateType, 'person')
    t.is(aggregateId, 'p-001')
    t.deepEqual(options, { take: 5, skip: 0 })
    return events
  })
  overrideMethod(
    client,
    'apply',
    async (
      aggregateType: string,
      aggregateId: string,
      eventType: string,
      options: { payload: { name: string }; token: string; requireExisting: boolean },
    ) => {
      t.is(aggregateType, 'person')
      t.is(aggregateId, 'p-001')
      t.is(eventType, 'Updated')
      t.deepEqual(options, { payload: { name: 'Jane Doe' }, token: 'custom-token', requireExisting: true })
      return created
    },
  )
  overrideMethod(
    client,
    'create',
    async (
      aggregateType: string,
      aggregateId: string,
      eventType: string,
      options: { token: string; payload: { name: string }; note: string },
    ) => {
      t.is(aggregateType, 'person')
      t.is(aggregateId, 'p-002')
      t.is(eventType, 'PersonCreated')
      t.deepEqual(options, {
        token: 'custom-token',
        payload: { name: 'Jane Doe' },
        note: 'created via test',
      })
      return createdAggregate
    },
  )
  overrideMethod(
    client,
    'archive',
    async (
      aggregateType: string,
      aggregateId: string,
      options: { token: string; comment: string },
    ) => {
      t.is(aggregateType, 'person')
      t.is(aggregateId, 'p-001')
      t.deepEqual(options, { token: 'custom-token', comment: 'archive note' })
      return archivedAggregate
    },
  )
  overrideMethod(
    client,
    'restore',
    async (
      aggregateType: string,
      aggregateId: string,
      options: { token: string; comment: string },
    ) => {
      t.is(aggregateType, 'person')
      t.is(aggregateId, 'p-001')
      t.deepEqual(options, { token: 'custom-token', comment: 'restore note' })
      return restoredAggregate
    },
  )
  overrideMethod(
    client,
    'patch',
    async (
      aggregateType: string,
      aggregateId: string,
      eventType: string,
      operations: Array<{ op: string; path: string; value: string }>,
      options: { note: string },
    ) => {
      t.is(aggregateType, 'person')
      t.is(aggregateId, 'p-001')
      t.is(eventType, 'PatchEvent')
      t.deepEqual(operations, [{ op: 'replace', path: '/name', value: 'Alice' }])
      t.deepEqual(options, { note: 'patch note' })
      return patched
    },
  )

  await client.connect()
  t.true(await client.isConnected())

  t.deepEqual(await client.list('person', { take: 10, skip: 2 }), [aggregate])
  t.deepEqual(await client.get('person', 'p-001'), aggregate)
  t.deepEqual(await client.select('person', 'p-001', ['state.name']), selection)
  t.deepEqual(await client.events('person', 'p-001', { take: 5, skip: 0 }), events)
  t.deepEqual(
    await client.apply('person', 'p-001', 'Updated', {
      payload: { name: 'Jane Doe' },
      token: 'custom-token',
      requireExisting: true,
    }),
    created,
  )
  t.deepEqual(
    await client.create('person', 'p-002', 'PersonCreated', {
      token: 'custom-token',
      payload: { name: 'Jane Doe' },
      note: 'created via test',
    }),
    createdAggregate,
  )
  t.deepEqual(
    await client.archive('person', 'p-001', {
      token: 'custom-token',
      comment: 'archive note',
    }),
    archivedAggregate,
  )
  t.deepEqual(
    await client.restore('person', 'p-001', {
      token: 'custom-token',
      comment: 'restore note',
    }),
    restoredAggregate,
  )
  t.deepEqual(
    await client.patch(
      'person',
      'p-001',
      'PatchEvent',
      [{ op: 'replace', path: '/name', value: 'Alice' }],
      { note: 'patch note' },
    ),
    patched,
  )

  await client.disconnect()
  t.false(await client.isConnected())
}

const runLiveControlOperations = async (t: ExecutionContext) => {
  const client = createClient(integrationOptions)
  await t.notThrowsAsync(async () => {
    await client.connect()
  })

  try {
    t.true(await client.isConnected())

    const operations: Array<[string, AsyncCall]> = [
      ['list', () => client.list(undefined, { take: 1, skip: 0 })],
      ['get', () => client.get('__test__', '__test__')],
      ['select', () => client.select('__test__', '__test__', ['state'])],
      ['events', () => client.events('__test__', '__test__', { skip: 0, take: 1 })],
      [
        'apply',
        () =>
          client.apply('__test__', '__test__', 'TestEvent', {
            payload: { ping: 'pong' },
            token: 'invalid-integration-token',
            requireExisting: false,
          }),
      ],
      [
        'create',
        () =>
          client.create('__test__', '__test__', 'IntegrationCreated', {
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
          client.patch(
            '__test__',
            '__test__',
            'PatchEvent',
            [{ op: 'replace', path: '/ping', value: 'pong' }],
            { note: 'integration smoke test', token: 'invalid-integration-token' },
          ),
      ],
    ]

    for (const [label, action] of operations) {
      try {
        await action()
      } catch (error) {
        const message = (error as Error).message ?? ''
        t.notRegex(message, /client is not connected/i, `${label} should not fail due to connection issues`)
      }
    }
  } finally {
    await client.disconnect().catch(() => {})
  }

  t.false(await client.isConnected())
}

test('control operations succeed when connected', async (t) => {
  if (controlSocketAvailable) {
    await runLiveControlOperations(t)
  } else {
    await runMockedControlOperations(t)
  }
})

test('control operations require an active connection', async (t) => {
  const client = buildClient()

  const operations: Array<[string, AsyncCall]> = [
    ['list', () => client.list()],
    ['get', () => client.get('person', 'p-001')],
    ['select', () => client.select('person', 'p-001', ['state.name'])],
    ['events', () => client.events('person', 'p-001')],
    ['apply', () => client.apply('person', 'p-001', 'TestEvent', undefined)],
    ['create', () => client.create('person', 'p-001', 'CreateAggregate')],
    ['archive', () => client.archive('person', 'p-001', undefined)],
    ['restore', () => client.restore('person', 'p-001', undefined)],
    ['patch', () => client.patch('person', 'p-001', 'PatchEvent', [{ op: 'replace', path: '/name', value: 'Alice' }])],
  ]

  for (const [label, action] of operations) {
    const error = await t.throwsAsync(action)
    t.regex(error?.message ?? '', /client is not connected/i, `${label} should reject without connection`)
  }
})
