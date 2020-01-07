import { Collection, ObjectId, BulkWriteOpResultObject } from 'mongodb'

export interface VersionedDocument {
  _v: number
}

interface SingleDocumentUpdater<T> {
  update(document: T): T | Promise<T>
}

interface DocumentBatchUpdater<T> {
  updateMany(documents: T[]): T[] | Promise<T[]>
}

export type SchemaRevision<T> = SingleDocumentUpdater<T> | DocumentBatchUpdater<T>

type SchemaEnforcer<T, H> = {
  (instance: T | H, persist?: (documents: T[]) => Promise<any>): Promise<T>
  (instances: (T | H)[], persist?: (documents: T[]) => Promise<any>): Promise<T[]>
  (instance: Promise<T | H>, persist?: (documents: T[]) => Promise<any>): Promise<T>
  (instances: Promise<(T | H)[]>, persist?: (documents: T[]) => Promise<any>): Promise<T[]>
}

type Input<T> = T | T[] | Promise<T | T[]>

const createSchema = <T extends VersionedDocument, H extends VersionedDocument>(revisions: SchemaRevision<T | H>[]) : SchemaEnforcer<T, H> => {
  const schemaVersion = revisions.length

  const updateDocuments: SchemaEnforcer<T, H> = async (input: Input<T | H>, persist?: (documents: T[]) => Promise<any>): Promise<any> => {
    input = await input

    let singleDocument = false
    if (!Array.isArray(input)) {
      singleDocument = true
      input = [ input ]
    }

    const documentsByVersion: { [version: string]: { indices: number[], documents: (T | H)[] } } = {}
    for (const i in input) {
      const document = input[i]
      const version = document ? document._v : schemaVersion

      if (!documentsByVersion[version]) {
        documentsByVersion[version] = { indices: [], documents: [] }
      }
      documentsByVersion[version].indices.push(+i)
      documentsByVersion[version].documents.push(document)
    }

    // @ts-ignore
    for (let version = Math.min(...Object.keys(documentsByVersion)); version < schemaVersion; version++) {
      const revision: SchemaRevision<T | H> = revisions[version]
      let { documents } = documentsByVersion[version]
      const { indices } = documentsByVersion[version]

      if ('updateMany' in revision) {
        documents = await revision.updateMany(documents)
      } else {
        documents = await Promise.all(documents.map((document: T | H) => revision.update(document)))
      }

      documents.forEach((document: T | H) => {
        if (document._v !== version + 1) {
          throw new Error(`Version missmatch on ${JSON.stringify(document)}. Expected version ${version}, received ${document._v}`)
        }
      })

      if (version === schemaVersion - 1 && persist) {
        await persist(<T[]>documents)
      }

      const nextVersionDocuments = documentsByVersion[version + 1] || { indices: [], documents: [] }
      const result = documentsByVersion[version + 1] = { indices: [], documents: [] }
      const documentCount = documents.length + nextVersionDocuments.documents.length

      let nVIndex: number = nextVersionDocuments.indices[0],
        cDIndex: number = indices[0]
      for (let i = 0; i < documentCount; i++) {
        if (typeof cDIndex === 'undefined') {
          result.documents.push(...nextVersionDocuments.documents)
          result.indices.push(...nextVersionDocuments.indices)
          break
        } else if (typeof nVIndex === 'undefined') {
          result.documents.push(...documents)
          result.indices.push(...indices)
          break
        }

        if (nVIndex < cDIndex) {
          result.documents.push(nextVersionDocuments.documents.shift())
          result.indices.push(nextVersionDocuments.indices.shift())
          nVIndex = nextVersionDocuments.indices[0]
        } else {
          result.documents.push(documents.shift())
          result.indices.push(indices.shift())
          cDIndex = indices[0]
        }
      }
    }

    const result: T[] = <T[]>documentsByVersion[schemaVersion].documents

    if (singleDocument) {
      return result[0]
    }

    return result
  }

  return updateDocuments
}

export default createSchema

interface Document {
  _id: ObjectId
  [others: string]: any
}

export const persistById = (collection: Collection) =>
  (documents: Document[]): Promise<BulkWriteOpResultObject> => collection.bulkWrite(documents.map((document) => ({
    replaceOne: {
      filter: { _id: document._id },
      replacement: document
    }
  })))

export const persistEmbeddedDocument = (collection: Collection, baseDocuments: Document | Document[], fieldName: string) => {
  if (!Array.isArray(baseDocuments)) {
    baseDocuments = [ baseDocuments ]
  }

  return (documents: any): Promise<BulkWriteOpResultObject> => collection.bulkWrite(documents.map((document: any, index: number) => ({
    updateOne: {
      filter: { _id: baseDocuments[index]._id },
      update: {
        $set: { [fieldName]: document }
      }
    }
  })))
}
