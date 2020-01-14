import { MongoClient, Db, Collection, ObjectId } from 'mongodb'
import createSchema, { SchemaRevision, VersionedEmbeddedDocument, VersionedDocument, Projection } from './index'

declare global {
  namespace NodeJS {
    interface Global {
      MONGO_URI: string
      MONGO_DB_NAME: string
    }
  }
}

const falsyValues = [ undefined, null, false ]

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

    await expect(Schema({ _v: 0, _id: new ObjectId() })).rejects.toThrow()
  })

  it('chains the update functions by passing them the previous return and the db instance', async () => {
    const document: VersionedDocument = { _v: 0, _id: new ObjectId() }

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
      _v: 1,
      _id: new ObjectId()
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
      _v: 0,
      _id: new ObjectId(),
      oldProperty: true
    }

    const newDocument: D = {
      _v: 1,
      _id: new ObjectId(),
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
      { _v: 0, _id: new ObjectId() },
      { _v: 2, _id: new ObjectId() },
      { _v: 1, _id: new ObjectId() },
      { _v: 1, _id: new ObjectId() }
    ]

    await expect(Schema(documents)).resolves.toEqual([
      {
        _v: 2,
        _id: documents[0]._id,
        updates: [ 1, 2 ]
      },
      {
        _v: 2,
        _id: documents[1]._id,
      },
      {
        _v: 2,
        _id: documents[2]._id,
        updates: [ 2 ]
      },
      {
        _v: 2,
        _id: documents[3]._id,
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

  it('can persist updates to multiple documents', async () => {
    const genRand = (): Number => Math.round(Math.random() * 100) + 1

    interface D_0 extends VersionedDocument {
      _v: 0
      _id: ObjectId
      a: number
      b: number
    }

    interface D_1 extends VersionedDocument {
      _v: 1
      _id: ObjectId
      prod: number
    }

    interface D extends VersionedDocument {
      _v: 2
      _id: ObjectId
      negProd: number
    }

    await collection.insertMany(<(D | D_1 | D_0)[]>[
      {
        _v: 0,
        _id: new ObjectId(),
        a: genRand(),
        b: genRand()
      },
      {
        _v: 1,
        _id: new ObjectId(),
        prod: genRand()
      }
    ])

    const revisions: SchemaRevision<D_0 | D_1 | D>[] = [ {
      updateMany: (documents: D_0[]) => documents.map(({ a, b, ...document }): D_1 => ({ ...document, prod: a * b, _v: 1 }))
    }, {
      updateMany: (documents: D_1[]) => documents.map(({ prod, ...document }): D => ({ ...document, negProd: -prod, _v: 2 }))
    } ]
    const Schema = createSchema(revisions)

    const documents = await Schema(collection.find({}).toArray(), collection)

    await expect(collection.find({}).toArray()).resolves.toEqual(documents)
  })

  it('returns the value, if a falsy value is passed', async () => {
    const Schema = createSchema([])
    
    for (const value of falsyValues) {
      await expect(Schema(<any>value)).resolves.toEqual(value)
    }

    await expect(Schema(<any[]>falsyValues)).resolves.toEqual(falsyValues)
  })

  it('supports empty arrays', async () => {
    const Schema = createSchema([])

    await expect(Schema([])).resolves.toEqual([])
  })

  describe('embedded documents', () => {
    interface E_0 extends VersionedEmbeddedDocument {
      _v: 0
      value: number
    }

    interface E extends VersionedEmbeddedDocument {
      _v: 1
      dValue: number
    }

    interface D extends VersionedDocument {
      readonly _v: 0
      value: number
      embedded?: E | E_0 // testing purposes, normally only E
    }

    it('works with a single base document', async () => {
      const document: D = {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 0,
          value: Math.random() * 10 + 1
        }
      }

      const update = (document: E_0): E => ({ _v: 1, dValue: document.value * 2 })
      const rawEmbeddedSchema = createSchema<E, E_0>([ { update } ], 'embedded')
      const EmbeddedSchema = <any>jest.fn().mockImplementation(rawEmbeddedSchema)
      EmbeddedSchema.schemaVersion = rawEmbeddedSchema.schemaVersion

      const Schema = createSchema<D, D>([], { embedded: EmbeddedSchema })

      await collection.insertOne(document)

      const updatedDocument = await Schema(collection.findOne({}), collection)

      expect(EmbeddedSchema).toHaveBeenCalledTimes(1)
      expect(EmbeddedSchema).toHaveBeenCalledWith([ document.embedded ])

      expect(updatedDocument.embedded).toEqual((await EmbeddedSchema.mock.results[0].value)[0])

      await expect(collection.findOne({})).resolves.toEqual(updatedDocument)
    })

    it('supports falsy values', async () => {
      const update = (document: E_0): E => ({ _v: 1, dValue: document.value * 2 })
      const rawEmbeddedSchema = createSchema<E, E_0>([ { update } ], 'embedded')
      const EmbeddedSchema = <any>jest.fn().mockImplementation(rawEmbeddedSchema)
      EmbeddedSchema.schemaVersion = rawEmbeddedSchema.schemaVersion

      const Schema = createSchema<D, D>([], { embedded: EmbeddedSchema })

      for (const falsyValue of falsyValues) {
        await expect(Schema(<any>falsyValue, collection)).resolves.toBe(falsyValue)
      }
      await expect(Schema(<any[]>falsyValues, collection)).resolves.toEqual(falsyValues)

      expect(EmbeddedSchema).toHaveBeenCalledTimes(falsyValues.length + 1)
    })

    it('works with multiple base documents', async () => {
      const documents: D[] = [ {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 0,
          value: Math.random() * 10 + 1
        }
      }, {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 0,
          value: Math.random() * 10 + 1
        }
      } ]

      const { embedded: e0 } = documents[0]
      const { embedded: e1 } = documents[1]

      const update = (document: E_0): E => ({ _v: 1, dValue: document.value * 2 })
      const rawEmbeddedSchema = createSchema<E, E_0>([ { update } ], 'embedded')
      const EmbeddedSchema = <any>jest.fn().mockImplementation(rawEmbeddedSchema)
      EmbeddedSchema.schemaVersion = rawEmbeddedSchema.schemaVersion

      const Schema = createSchema<D, D>([], { embedded: EmbeddedSchema })

      await collection.insertMany(documents)

      const updatedDocuments = await Schema(documents, collection)

      expect(EmbeddedSchema).toHaveBeenCalledTimes(1)
      expect(EmbeddedSchema).toHaveBeenCalledWith([ e0, e1 ]) // documents get mutated

      expect(updatedDocuments).toEqual([
        expect.objectContaining({ embedded: (await EmbeddedSchema.mock.results[0].value)[0] }),
        expect.objectContaining({ embedded: (await EmbeddedSchema.mock.results[0].value)[1] })
      ])

      await expect(collection.find({}).toArray()).resolves.toEqual(updatedDocuments)
    })

    it('does not overwrite missing embedded documents when passing a collection and deletes removed keys', async () => {
      const documents: D[] = [ {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 0,
          value: Math.random() * 10 + 1
        }
      }, {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 0,
          value: Math.random() * 10 + 1
        }
      } ]

      interface D_next extends VersionedDocument {
        readonly _v: 1
        _id: ObjectId
        embedded?: E | E_0
      }

      const updateEmbedded = (document: E_0): E => ({ _v: 1, dValue: document.value * 2 })
      const rawEmbeddedSchema = createSchema<E, E_0>([ { update: updateEmbedded } ], 'embedded')
      const EmbeddedSchema = <any>jest.fn().mockImplementation(rawEmbeddedSchema)
      EmbeddedSchema.schemaVersion = rawEmbeddedSchema.schemaVersion

      const update = (document: D): D_next => ({ _id: document._id, _v: 1 })
      const Schema = createSchema<D_next, D>([ { update } ], { embedded: EmbeddedSchema })

      await collection.insertMany(documents)

      const projection: Projection = { embedded: false }
      const updatedDocuments = await Schema(collection.find({}, { projection }).toArray(), collection, projection)

      expect(EmbeddedSchema).toHaveBeenCalledTimes(0)

      await expect(collection.find({}).toArray()).resolves.toEqual(
        updatedDocuments.map((document: D_next, index: number): D_next => ({ ...document, embedded: documents[index].embedded }))
      )
    })

    it('works when nothing needs to be updated', async () => {
      const documents: D[] = [ {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 1,
          dValue: Math.random() * 10 + 1
        }
      }, {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 1,
          dValue: Math.random() * 10 + 1
        }
      } ]

      const update = (document: E_0): E => ({ _v: 1, dValue: document.value * 2 })
      const rawEmbeddedSchema = createSchema<E, E_0>([ { update } ], 'embedded')
      const EmbeddedSchema = <any>jest.fn().mockImplementation(rawEmbeddedSchema)
      EmbeddedSchema.schemaVersion = rawEmbeddedSchema.schemaVersion

      const Schema = createSchema<D, D>([], { embedded: EmbeddedSchema })

      await collection.insertMany(documents)

      const updatedDocuments = await Schema(documents, collection)

      expect(EmbeddedSchema).toHaveBeenCalledTimes(1)
      expect(EmbeddedSchema).toHaveBeenCalledWith([ documents[0].embedded, documents[1].embedded ])

      expect(updatedDocuments).toEqual([
        expect.objectContaining({ embedded: (await EmbeddedSchema.mock.results[0].value)[0] }),
        expect.objectContaining({ embedded: (await EmbeddedSchema.mock.results[0].value)[1] })
      ])

      await expect(collection.find({}).toArray()).resolves.toEqual(updatedDocuments)
    })

    it('throws when a whitelist projection is passed', async () => {
      const documents: D[] = [ {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 0,
          value: Math.random() * 10 + 1
        }
      }, {
        _v: 0,
        _id: new ObjectId(),
        value: 0,
        embedded: {
          _v: 0,
          value: Math.random() * 10 + 1
        }
      } ]

      interface D_next extends VersionedDocument {
        readonly _v: 1
        _id: ObjectId
        embedded?: E | E_0
      }

      const updateEmbedded = (document: E_0): E => ({ _v: 1, dValue: document.value * 2 })
      const rawEmbeddedSchema = createSchema<E, E_0>([ { update: updateEmbedded } ], 'embedded')
      const EmbeddedSchema = <any>jest.fn().mockImplementation(rawEmbeddedSchema)
      EmbeddedSchema.schemaVersion = rawEmbeddedSchema.schemaVersion

      const update = (document: D): D_next => ({ _id: document._id, _v: 1 })
      const Schema = createSchema<D_next, D>([ { update } ], { embedded: EmbeddedSchema })

      await collection.insertMany(documents)

      // @ts-ignore
      const projection: Projection = { _id: true, _v: true }
      await expect(Schema(collection.find({}, { projection }).toArray(), collection, projection)).rejects.toThrow()
    })
  })
})
