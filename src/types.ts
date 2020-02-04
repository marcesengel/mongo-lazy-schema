import { ObjectId, Collection } from 'mongodb'

export interface VersionedEmbeddedDocument {
  _v: number
}

export interface VersionedDocument extends VersionedEmbeddedDocument {
  _id: ObjectId
}

interface SingleDocumentUpdater<T> {
  update(document: T): T | Promise<T>
}

interface DocumentBatchUpdater<T> {
  updateMany(documents: T[]): T[] | Promise<T[]>
}

export type SchemaRevision<T> = SingleDocumentUpdater<T> | DocumentBatchUpdater<T>

export interface SchemaMetaProvider {
  schemaVersion: number
}

export interface SchemaEnforcer<T, H> extends SchemaMetaProvider {
  (instance: T | H, collection?: Collection, projection?: Projection<T>): Promise<T>
  (instances: (T | H)[], collection?: Collection, projection?: Projection<T>): Promise<T[]>
  (instance: Promise<T | H>, collection?: Collection, projection?: Projection<T>): Promise<T>
  (instances: Promise<(T | H)[]>, collection?: Collection, projection?: Projection<T>): Promise<T[]>
}

export interface EmbeddedSchemaEnforcer<T, H> extends SchemaMetaProvider {
  (instance: T | H): Promise<T>
  (instances: (T | H)[]): Promise<T[]>
  (instance: Promise<T | H>): Promise<T>
  (instances: Promise<(T | H)[]>): Promise<T[]>
}

export type Input<T> = T | T[] | Promise<T | T[]>

export interface DocumentMetaData {
  index: number
  willBeUpdated: boolean
  initialFields?: string[]
}

export type Projection<T> = {
  [key in keyof T]?: {} extends Pick<T, key> ? false : never
}
