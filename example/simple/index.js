const { Kafka, AuthenticationMechanisms } = require('kafkajs')
const { Mechanism, Type } = require('../../src')
AuthenticationMechanisms[Type] = () => Mechanism

const kafka = new Kafka({
  brokers: process.env.BROKERS.split(','),
  clientId: 'consumer',
  ssl: true,
  sasl: {
    mechanism: Type,
    region: process.env.REGION,
    ttl: process.env.TTL
  }
})

async function run () {
  const admin = kafka.admin()
  await admin.connect()
  const topics = await admin.listTopics()
  console.log('Topics: ', topics)
  const producer = kafka.producer()

await producer.connect()
await producer.send({
    topic: 'topic-name'
})
  await admin.disconnect()
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Error: ', err)
    process.exit(1)
  })
