import { VersionedDocument, Input, Projection, EmbeddedSchemaEnforcer, SchemaRevision, DocumentMetaData } from './types'
import { Collection } from 'mongodb'

import transformInput from './transformInput'
import persistChangedDocuments from './persistChangedDocuments'

export interface Context<T> {
  embedded: boolean
  embeddedDocumentSchemas: { [key: string]: EmbeddedSchemaEnforcer<any, any> }
  embeddedSchemaVersions: { [key: string]: number }
  hasEmbeddedDocuments: boolean
  schemaVersion: number
  revisions: SchemaRevision<T>[]
}

export default async function updatedDocuments<T extends VersionedDocument, H extends VersionedDocument> (
  this: Context<T | H>, input: Input<T | H>, collection?: Collection, projection: Projection = {}
): Promise<any> {
  input = await input

  let singleDocument = false
  if (!Array.isArray(input)) {
    singleDocument = true
    input = [ input ]
  }

  validateProjection(projection)

  if (input.length === 0) {
    return []
  }

  const projectedEmbeddedDocumentNames = Object.keys(this.embeddedDocumentSchemas).filter(
    (fieldName) => projection[fieldName] !== false
  )

  const { documentsByVersion, embeddedDocuments } = transformInput.call(this, input, projection)

  let updatedEmbeddedDocuments = {}  

  if (!this.embedded) {
    const updatedEmbeddedDocumentsList = await Promise.all(projectedEmbeddedDocumentNames.map(
      (embeddedDocumentName) => this.embeddedDocumentSchemas[embeddedDocumentName](embeddedDocuments[embeddedDocumentName])
    ))

    updatedEmbeddedDocuments = updatedEmbeddedDocumentsList.reduce(
      (updatedEmbeddedDocuments, updatedDocuments, index) => {
        updatedEmbeddedDocuments[
          projectedEmbeddedDocumentNames[index]
        ] = updatedDocuments

        return updatedEmbeddedDocuments
      }, {}
    )
  }

  // @ts-ignore
  for (let version = Math.min(...Object.keys(documentsByVersion)); version < this.schemaVersion; version++) {
    const revision: SchemaRevision<T | H> = this.revisions[version]
    let { documents } = documentsByVersion[version]
    const { metaData } = documentsByVersion[version]

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

    const nextVersionDocuments = documentsByVersion[version + 1] || { metaData: [], documents: [] }
    const result = documentsByVersion[version + 1] = { metaData: [], documents: [] }
    const documentCount = documents.length + nextVersionDocuments.documents.length

    const getIndex = (metaData: DocumentMetaData[]) => metaData[0] && metaData[0].index

    let nVIndex: number = getIndex(nextVersionDocuments.metaData),
      cDIndex: number = getIndex(metaData)
    for (let i = 0; i < documentCount; i++) {
      if (typeof cDIndex === 'undefined') {
        result.documents.push(...nextVersionDocuments.documents)
        result.metaData.push(...nextVersionDocuments.metaData)
        break
      } else if (typeof nVIndex === 'undefined') {
        result.documents.push(...documents)
        result.metaData.push(...metaData)
        break
      }

      if (nVIndex < cDIndex) {
        result.documents.push(nextVersionDocuments.documents.shift())
        result.metaData.push(nextVersionDocuments.metaData.shift())
        nVIndex = getIndex(nextVersionDocuments.metaData)
      } else {
        result.documents.push(documents.shift())
        result.metaData.push(metaData.shift())
        cDIndex = getIndex(metaData)
      }
    }
  }

  const result: T[] = <T[]>documentsByVersion[this.schemaVersion].documents

  if (!this.embedded && projectedEmbeddedDocumentNames.length > 0) {
    for (const embeddedDocumentName of projectedEmbeddedDocumentNames) {
      for (const index in result) {
        if (!result[index]) {
          continue
        }

        result[index][embeddedDocumentName] = updatedEmbeddedDocuments[embeddedDocumentName][index]
      }
    }
  }

  if (collection && !this.embedded) {
    await persistChangedDocuments.call(this, documentsByVersion[this.schemaVersion].documents, documentsByVersion[this.schemaVersion].metaData, collection)
  }

  if (singleDocument) {
    return result[0]
  }

  return result
}

const validateProjection = (projection: Projection): void =>  {
  for (const key in projection) {
    if (projection[key] !== false) {
      throw new Error('mongo-lazy-schema currently only supports excluding projections.')
    }
  }
}
