import { VersionedEmbeddedDocument, DocumentMetaData, Projection } from './types'

import { Context } from './updateDocuments'

interface DocumentsByVersion<T, H> {
  [version: string]: {
    documents: (T | H)[],
    metaData: DocumentMetaData[]
  }
}

interface TransformedInput<T, H> {
  documentsByVersion: DocumentsByVersion<T, H>,
  embeddedDocuments: {
    [key: string]: object[]
  }
}

export default function transformInput<T extends VersionedEmbeddedDocument, H extends VersionedEmbeddedDocument> (
  this: Context<T | H>,
  input: (T | H)[],
  projection: Projection<T>
): TransformedInput<T, H> {
  const projectedEmbeddedDocumentNames = Object.keys(this.embeddedDocumentSchemas).filter(
    (fieldName) => projection[fieldName] !== false
  )

  const documentsByVersion: DocumentsByVersion<T, H> = {}
  const embeddedDocuments = projectedEmbeddedDocumentNames.reduce(
    (result, documentName) => {
      result[documentName] = []
      return result
    }, {}
  )

  for (const i in input) {
    const document = input[i]
    const version = document ? document._v : this.schemaVersion
    let documentWillBeUpdated = version < this.schemaVersion

    if (!documentsByVersion[version]) {
      documentsByVersion[version] = { documents: [], metaData: [] }
    }

    for (const embeddedDocumentName of projectedEmbeddedDocumentNames) {
      const embeddedDocument: VersionedEmbeddedDocument = document && document[embeddedDocumentName]
      if (embeddedDocument && !documentWillBeUpdated && embeddedDocument._v < this.embeddedSchemaVersions[embeddedDocumentName]) {
        documentWillBeUpdated = true
      }

      embeddedDocuments[embeddedDocumentName].push(embeddedDocument)
    }

    documentsByVersion[version].documents.push(document)
    documentsByVersion[version].metaData.push({
      index: +i,
      willBeUpdated: documentWillBeUpdated,
      initialFields: this.hasEmbeddedDocuments ? (document && Object.keys(document)) : undefined
    })
  }

  return {
    documentsByVersion,
    embeddedDocuments
  }
}
