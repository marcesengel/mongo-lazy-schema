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

  it('throws an error when a bad version is returned by an updater', async () => {
    const revisions: SchemaRevision<VersionedDocument>[] = [ { update: (entity) => entity } ]
    const Schema = createSchema(revisions)

    await expect(Schema({ _id: '1', _v: 0 })).rejects.toThrow()
  })

  it('chains the update functions by passing them the previous return and the db instance', async () => {
    const document: VersionedDocument = { _id: '1', _v: 0 }

    const revisions = []
    for (let i = 0; i < 2; i++) {
      revisions.push({
        update: jest.fn().mockResolvedValue({
          ...document,
          _v: i + 1,
          ['property' + (i + 1)]: true
        })
      })
    }

    const Schema = createSchema(revisions)
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
    const document: VersionedDocument = {
      _id: '1',
      _v: 1
    }

    const revisions = [
      {
        update: jest.fn().mockReturnValue({ ...document, _v: 1 })
      },
      {
        update: jest.fn().mockReturnValue({ ...document, _v: 2 })
      }
    ]
  
    const Schema = createSchema(revisions)
  
    await Schema(document)
  
    expect(revisions[0].update).not.toHaveBeenCalled()
    expect(revisions[1].update).toHaveBeenCalledTimes(1)
  })
  
  it('returns what was returned by the last update()', async () => {
    interface D_1 extends VersionedDocument {
      oldProperty: boolean
    }

    interface D extends VersionedDocument {
      _v: 1
      newProperty: true
    }

    const oldDocument: D_1 = {
      _id: '1',
      _v: 0,
      oldProperty: true
    }

    const newDocument: D = {
      _id: '1',
      _v: 1,
      newProperty: true
    }

    const Schema = createSchema<D, D_1>([ { update: (document: D_1): D => newDocument } ])

    await expect(Schema(oldDocument)).resolves.toEqual(newDocument)
  })

  it('makes proper use of updateMany', async () => {
    const makeBatchUpdater = (propIndex: number) =>
      (documents) => documents.map(({ _v, ...document }) => ({ ...document, _v: _v + 1, updates: (document.updates || []).concat(propIndex) }))

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
      { _id: '3', _v: 1 },
      { _id: '4', _v: 1 }
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
      },
      {
        _id: '4',
        _v: 2,
        updates: [ 2 ]
      }
    ])

    expect(revisions[0].updateMany).toHaveBeenCalledTimes(1)
    expect(revisions[0].updateMany).toHaveBeenCalledWith([ documents[0] ])

    expect(revisions[1].updateMany).toHaveBeenCalledTimes(1)
    expect(revisions[1].updateMany).toHaveBeenCalledWith(
      expect.arrayContaining([ documents[2], documents[3], ...revisions[0].updateMany.mock.results[0].value ])
    )
  })

  it('can persist updates to mulitple documents', async () => {
    const genRand = (): Number => Math.round(Math.random() * 100)

    interface D_0 extends VersionedDocument {
      _v: 0
      a: number
      b: number
    }

    interface D_1 extends VersionedDocument {
      _v: 1
      prod: number
    }

    interface D extends VersionedDocument {
      _v: 2
      negProd: number
    }

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

    const revisions: SchemaRevision<D_0 | D_1 | D>[] = [ {
      updateMany: (documents: D_0[]) => documents.map(({ a, b, ...document }): D_1 => ({ ...document, prod: a * b, _v: 1 }))
    }, {
      updateMany: (documents: D_1[]) => documents.map(({ prod, ...document }): D => ({ ...document, negProd: -prod, _v: 2 }))
    } ]
    const Schema = createSchema(revisions)

    const documents = await Schema(await collection.find({}).toArray(), collection)

    await expect(collection.find({}).toArray()).resolves.toEqual(documents)
  })
})