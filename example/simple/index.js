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
  const res = await admin.createTopics({
      topics: [
        { topic:'lime1'}, { topic:'test1'}]
    })
  console.log('res: ', JSON.stringify(res))
  
  console.log('Topics: ', topics)
  await admin.disconnect()
  
  const producer = kafka.producer()

await producer.connect()
setInterval(async () => {
  console.log('timeout')
                    await producer.send({
    topic: 'lime',
    messages: [{
        value: 'hello world'+ Date.now()
    }]
});
                }, 500);
  
   await producer.send({
    topic: 'lime',
    messages: [{
        value: 'hello world'+ Date.now()
    }]
});
  
  
  
  
  ///////////////////////////////////
  
  const topic = 'lime'
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
  
  
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Error: ', err)
    process.exit(1)
  })
