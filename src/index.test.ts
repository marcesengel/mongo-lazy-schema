import { MongoClient, Db, Collection } from 'mongodb'
import createSchema, { SchemaRevision, VersionedDocument } from './index'

declare global {
  namespace NodeJS {
    interface Global {
      MONGO_URI: string
      MONGO_DB_NAME: string
    }
  }
}

describe('createSchema', () => {
  let client: MongoClient
  let db: Db
  let collection: Collection

  beforeAll(async () => {
    client = await MongoClient.connect(global.MONGO_URI, { useUnifiedTopology: true })
    db = client.db(global.MONGO_DB_NAME)
    collection = await db.createCollection('test')
  })

  afterEach(async () => {
    await db.dropCollection('test')
    collection = await db.createCollection('test')
  })
   
  afterAll(async () => {
    await client.close()
  })

  it('adds the version to the object returned by the last update function', async () => {
    const revisions: SchemaRevision[] = [ { update: () => ({}) } ]
    const Schema = createSchema(revisions)

    await expect(Schema({ _id: '1', _v: 0 })).resolves.toHaveProperty('_v', revisions.length)
  })

  it('chains the update functions by passing them the previous return and the db instance', async () => {
    const revisions = []
    for (let i = 0; i < 2; i++) {
      revisions.push({
        update: jest.fn().mockResolvedValue({
          ['property' + (i + 1)]: true
        })
      })
    }

    const Schema = createSchema(revisions)
    const document: VersionedDocument = { _id: '1', _v: 0 }
    await Schema(document)
    
    for (const i in revisions) {
      const previousReturn = +i === 0
        ? document
        : await revisions[+i - 1].update.mock.results[0].value
      
      const updateFnc = revisions[i].update
      expect(updateFnc).toHaveBeenCalledTimes(1)
      expect(updateFnc).toHaveBeenCalledWith(previousReturn)
    }
  })

  it('calls the proper update functions according to the current document version', async () => {
    const revisions = [
      {
        update: jest.fn()
      },
      {
        update: jest.fn().mockReturnValue({})
      }
    ]
  
    const Schema = createSchema(revisions)
  
    const document: VersionedDocument = {
      _id: '1',
      _v: 1
    }
  
    await Schema(document)
  
    expect(revisions[0].update).not.toHaveBeenCalled()
    expect(revisions[1].update).toHaveBeenCalledTimes(1)
  })
  
  it('returns a superset of what was returned by the last update()', async () => {
    const oldDocument: VersionedDocument = {
      _id: '1',
      _v: 0,
      oldProperty: true
    }

    const newDocument = {
      newProperty: true
    }

    const Schema = createSchema([ { update: () => Promise.resolve(newDocument) } ])

    await expect(Schema(oldDocument)).resolves.toEqual(
      expect.objectContaining(newDocument)
    )
  })

  it('makes proper use of updateMany', async () => {
    const makeBatchUpdater = (propIndex: number) =>
      (documents) => documents.map((document) => ({ ...document, updates: (document.updates || []).concat(propIndex) }))

    const revisions = [
      {
        updateMany: jest.fn(makeBatchUpdater(1))
      },
      {
        updateMany: jest.fn(makeBatchUpdater(2))
      }
    ]
    const Schema = createSchema(revisions)

    const documents: VersionedDocument[] = [
      { _id: '1', _v: 0 },
      { _id: '2', _v: 2 },
      { _id: '3', _v: 1 }
    ]

    await expect(Schema(documents)).resolves.toEqual([
      {
        _id: '1',
        _v: 2,
        updates: [ 1, 2 ]
      },
      {
        _id: '2',
        _v: 2
      },
      {
        _id: '3',
        _v: 2,
        updates: [ 2 ]
      }
    ])

    expect(revisions[0].updateMany).toHaveBeenCalledTimes(1)
    expect(revisions[0].updateMany).toHaveBeenCalledWith([ documents[0] ])

    expect(revisions[1].updateMany).toHaveBeenCalledTimes(1)
    expect(revisions[1].updateMany).toHaveBeenCalledWith(
      expect.arrayContaining([ documents[2], ...revisions[0].updateMany.mock.results[0].value ])
    )
  })

  it('can persist updates to mulitple documents', async () => {
    const genRand = (): Number => Math.round(Math.random() * 100)

    await collection.insertMany([
      {
        _v: 0,
        a: genRand(),
        b: genRand()
      },
      {
        _v: 1,
        prod: genRand()
      }
    ])

    const revisions: SchemaRevision[] = [ {
      updateMany: (documents) => documents.map(({ a, b, ...document }) => ({ prod: a * b, _v: 2, ...document }))
    }, {
      updateMany: (documents) => documents.map(({ prod, ...document }) => ({ negProd: -prod, ...document }))
    } ]
    const Schema = createSchema(revisions)

    const documents = await Schema(await collection.find({}).toArray(), collection)

    await expect(collection.find({}).toArray()).resolves.toEqual(documents)
  })
})