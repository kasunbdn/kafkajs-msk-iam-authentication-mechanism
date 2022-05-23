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
  
  const consumer = kafka.consumer({ groupId: 'test-group'+ Date.now()})
  await consumer.subscribe({ topic: 'lime',fromBeginning: true})
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        console.log({
          value: message.value.toString(),
          topic
        })
      }
    });
   await producer.send({
    topic: 'lime',
    messages: [{
        value: 'hello world'+ Date.now()
    }]
});
  
  
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Error: ', err)
    process.exit(1)
  })
