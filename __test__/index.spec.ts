import test from 'ava'

import { createClient } from '../index.js'

test('native module exports createClient helper', (t) => {
  const client = createClient()
  t.truthy(client)
  t.is(typeof client.connect, 'function')
  t.is(typeof client.disconnect, 'function')
  t.deepEqual(client.endpoint, { ip: '127.0.0.1', port: 6363 })
})
