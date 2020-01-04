import { Collection, ObjectId } from 'mongodb'

export interface VersionedDocument {
  _id: ObjectId
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
  (instance: T | H, collection?: Collection): Promise<T>
  (instances: (T | H)[], collection?: Collection): Promise<T[]>
  (instance: Promise<T | H>, collection?: Collection): Promise<T>
  (instances: Promise<(T | H)[]>, collection?: Collection): Promise<T[]>
}

type Input<T> = T | T[] | Promise<T | T[]>

const createSchema = <T extends VersionedDocument, H extends VersionedDocument>(revisions: SchemaRevision<T | H>[]) : SchemaEnforcer<T, H> => {
  const schemaVersion = revisions.length

  const updateDocuments: SchemaEnforcer<T, H> = async (input: Input<T | H>, collection?: Collection): Promise<any> => {
    input = await input

    if (!input) {
      return input
    }

    let singleDocument = false
    if (!Array.isArray(input)) {
      singleDocument = true
      input = [ input ]
    }

    const documentsByVersion = {}
    const documentIndexById = {}
    for (const i in input) {
      const document = input[i]
      const version = document._v

      documentIndexById[document._id.toHexString()] = i

      if (!documentsByVersion[version]) {
        documentsByVersion[version] = []
      }
      documentsByVersion[version].push(document)
    }

    const getIndex = (document: T | H) => document
      ? documentIndexById[document._id.toHexString()]
      : -1

    // @ts-ignore
    for (let version = Math.min(...Object.keys(documentsByVersion)); version < schemaVersion; version++) {
      const revision: SchemaRevision<T | H> = revisions[version]
      let documents = documentsByVersion[version]

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

      if (version === schemaVersion - 1 && collection) {
        await collection.bulkWrite(documents.map((document: T) => ({
          replaceOne: {
            filter: { _id: document._id },
            replacement: document
          }
        })))
      }

      const nextVersionDocuments = documentsByVersion[version + 1] || []
      const result = documentsByVersion[version + 1] = []
      const documentCount = documents.length + nextVersionDocuments.length

      let nVIndex = getIndex(nextVersionDocuments[0]),
        cDIndex = getIndex(documents[0])
      for (let i = 0; i < documentCount; i++) {
        if (cDIndex === -1) {
          result.push(...nextVersionDocuments)
          break
        } else if (nVIndex === -1) {
          result.push(...documents)
          break
        }

        if (nVIndex < cDIndex) {
          result.push(nextVersionDocuments.shift())
          nVIndex = getIndex(nextVersionDocuments[0])
        } else {
          result.push(documents.shift())
          cDIndex = getIndex(documents[0])
        }
      }
    }

    const result = documentsByVersion[schemaVersion]

    if (singleDocument) {
      return result[0]
    }

    return result
  }

  return updateDocuments
}

export default createSchema