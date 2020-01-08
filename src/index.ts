import { Collection } from 'mongodb'

import {
  VersionedBaseDocument, VersionedDocument, SchemaRevision, SchemaEnforcer,
  EmbeddedSchemaEnforcer, Input, Projection, DocumentMetaData
} from './types'

import updateDocuments, { Context } from './updateDocuments'

function createSchema<T extends VersionedBaseDocument, H extends VersionedBaseDocument>(
  revisions: SchemaRevision<T | H>[], embeddedDocumentSchemas?: { [key: string]: SchemaEnforcer<any, any> }
): SchemaEnforcer<T, H>

function createSchema<T extends VersionedDocument, H extends VersionedDocument>(
  revisions: SchemaRevision<T | H>[], embedded: 'embedded'
): EmbeddedSchemaEnforcer<T, H>

function createSchema<T extends VersionedBaseDocument, H extends VersionedBaseDocument>(
  revisions: SchemaRevision<T | H>[], embeddedDocumentSchemas?: 'embedded' | { [key: string]: SchemaEnforcer<any, any> }
): SchemaEnforcer<T, H> | EmbeddedSchemaEnforcer<T, H> {
  const schemaVersion = revisions.length

  const embedded = embeddedDocumentSchemas === 'embedded'
  const hasEmbeddedDocuments = !embedded && Object.keys(embeddedDocumentSchemas || {}).length > 0
  const embeddedSchemaVersions = !embedded && Object.keys(embeddedDocumentSchemas || {}).reduce(
    (result, embeddedSchemaName) => {
      result[embeddedSchemaName] = (<SchemaEnforcer<any, any>>embeddedDocumentSchemas[embeddedSchemaName]).schemaVersion

      return result
    }, {}
  )

  const context: Context<T | H> = {
    schemaVersion,
    embedded,
    embeddedDocumentSchemas: embeddedDocumentSchemas === 'embedded' ? {} : (embeddedDocumentSchemas || {}),
    hasEmbeddedDocuments,
    embeddedSchemaVersions,
    revisions
  }

  let updater: SchemaEnforcer<T, H> = updateDocuments.bind(context)
  updater.schemaVersion = schemaVersion

  return updater
}

export default createSchema

export { VersionedBaseDocument, VersionedDocument, SchemaRevision, Projection } from './types'
