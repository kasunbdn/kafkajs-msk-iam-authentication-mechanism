const { Kafka, AuthenticationMechanisms } = require('kafkajs')
const express = require('express')
const app = express()
const { Mechanism, Type } = require('../../src')
AuthenticationMechanisms[Type] = () => Mechanism

const port = process.env.PORT || 3000

const kafka = new Kafka({
  brokers: process.env.BROKERS.split(','),
  clientId: 'consumer',
  ssl: true,
  sasl: {
    mechanism: 'aws',
    authorizationIdentity: 'Kasun'
  }
})


/*
sasl: {
    mechanism: Type,
    region: process.env.REGION,
    ttl: process.env.TTL
  }*/

const producer = kafka.producer()
const admin = kafka.admin()

app.use(express.json())

app.use((req, res, next) => {
  console.log({
    body: req.body,
    path: req.path,
    method: req.method
  })
  next()
})

app.use((req, res, next) => {
  req.producer = producer
  next()
})
/*
app.post('/provision', async (req, res) => {
  try {
    const { body: { topics } } = req
    if (!topics) {
      return res.sendStatus(400)
    }
    const kafkaTopics = topics.map(topic => ({ topic }))
    await admin.connect()
    await admin.createTopics({
      topics: kafkaTopics
    })
  } catch (err) {
    console.error(err)
    res.sendStatus(500)
  }
})

app.post('/', async (req, res) => {
  try {
    const { body: { message, topic } } = req
    if (!message || !topic) {
      return res.sendStatus(400)
    }

    await req.producer.send({
      topic,
      messages: [
        { value: JSON.stringify(message) }
      ]
    })
    res.send('Hello World!')
  } catch (err) {
    console.error(err)
    res.sendStatus(500)
  }
})
*/

app.post('/topic', async (req, res) => {
  try {
    const { body: { name } } = req
    if (!name) {
      return res.sendStatus(400)
    }

    await admin.connect()
    await admin.createTopics({
      topics: [{
        topic: name
      }]
    })
    res.sendStatus(200)
  } catch (err) {
    console.error(err)
    res.sendStatus(500)
  }
})

app.post('/message', async (req, res) => {
  try {
    const { body: { name, topic } } = req
    if (!name || !topic) {
      return res.sendStatus(400)
    }

    await producer.send({
      topic,
      messages: [
        { value: JSON.stringify(name) }
      ]
    })
    res.sendStatus(200)
  } catch (err) {
    console.error(err)
    res.sendStatus(500)
  }
})

app.post('/subscribe', async (req, res) => {
  try {
    const { body: { topic } } = req
    if (!topic) {
      return res.sendStatus(400)
    }
const consumer = kafka.consumer({ groupId: 'test-group' })
await consumer.connect()
    await consumer.subscribe({ topic })
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        console.log({
          value: message.value.toString(),
          topic
        })
      }
    })
    res.sendStatus(200)
  } catch (err) {
    console.error(err)
    res.sendStatus(500)
  }
})
async function run () {
  await producer.connect()
  app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
  })
}

run().catch(console.error)
