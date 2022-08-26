import Fastify from 'fastify'
import { Client } from '@elastic/elasticsearch'
import fastifyCors from '@fastify/cors'

const fastify = Fastify({
  logger: process.env.LOGGER === 'on' ? true : false,
})

fastify.register(fastifyCors)

const client = new Client({
  node: process.env.ELASTICSEARCH_HOST,
})

fastify.get('/', async (req) => {
  // console.log(req.query)
  req.log.info(JSON.stringify(req.query))

  if (!req.query.q) {
    return { hits: [] }
  }

  const q = req.query.q.toLowerCase()

  const res = await client.search({
    "size": 1000,
    "query": {
      "bool": {
        "should": [
          {
            "wildcard": {
              "id": {
                "value": `*${q}*`
              }
            }
          },
          {
            "wildcard": {
              "label": {
                "value": `*${q}*`
              }
            }
          },
        ],
        "minimum_should_match": 1
      }
    }
  })
  // console.log(res.hits)
  return {
    hits: res.hits.hits
  }
})

const start = async () => {
  try {
    await fastify.listen(process.env.PORT, '0.0.0.0')
  } catch (e) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()