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
  console.log("ttl", process.env.TTL)
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
                    await producer.send({
    topic: 'lime',
    messages: [{
        key: 'key1',
        value: 'hello world'+ Date.now(),
        headers: {
            'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
            'system-id': 'my-system',
        }
    }]
});
                }, 500);
  
  const consumer = kafka.consumer({ groupId: 'test-group' })
  await consumer.subscribe({  topic:'lime' })
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
        key: 'key1',
        value: 'hello world'+ Date.now(),
        headers: {
            'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
            'system-id': 'my-system',
        }
    }]
});
  
  
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Error: ', err)
    process.exit(1)
  })
