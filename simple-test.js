const { createClient } = require('./index')

const client = createClient()

if (typeof client !== 'object') {
    throw new Error('createClient did not return an object')
}

console.info('Simple test passed')
