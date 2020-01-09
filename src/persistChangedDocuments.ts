import { Context } from './updateDocuments'
import { VersionedDocument, DocumentMetaData } from './types'
import { Collection } from 'mongodb'

export default async function persistChangedDocuments<T extends VersionedDocument> (
  this: Context<T>,
  documents: T[],
  metaData: DocumentMetaData[],
  collection: Collection
) {
  let bulkWriteOperations: object[]
  if (this.hasEmbeddedDocuments) {
    bulkWriteOperations = metaData.reduce<object[]>((operations, { willBeUpdated, initialFields }, index) => {
      if (willBeUpdated) {
        const document = documents[index]
        const update: { $set: object, $unset?: object } = {
          $set: document
        }

        const { op: shouldUnset, $unset } = initialFields.reduce(
          (result, fieldName) => {
            if (typeof document[fieldName] === 'undefined' && !this.embeddedDocumentSchemas[fieldName]) {
              result.op = true
              result.$unset[fieldName] = true
            }

            return result
          }, { op: false, $unset: {} }
        )

        if (shouldUnset) {
          update.$unset = $unset
        }

        operations.push({
          updateOne: {
            filter: { _id: document._id },
            update
          }
        })
      }

      return operations
    }, [])
  } else {
    bulkWriteOperations = metaData.reduce<object[]>((operations, { willBeUpdated }, index) => {
      if (willBeUpdated) {
        operations.push({
          replaceOne: {
            filter: { _id: documents[index]._id },
            replacement: documents[index]
          }
        })
      }

      return operations
    }, [])
  }

  if (bulkWriteOperations.length === 0) {
    return
  }

  return collection.bulkWrite(bulkWriteOperations)
}
