import { MongoClient, Db } from "mongodb";

type Cache = { client: MongoClient; db: Db };
const globalForMongo = globalThis as typeof globalThis & { rutasurMongo?: Promise<Cache> };

export async function getDb(): Promise<Db> {
  if (!globalForMongo.rutasurMongo) {
    const uri = process.env.MONGO_URI ?? "mongodb://localhost:27017/rutasur?directConnection=true";
    globalForMongo.rutasurMongo = new MongoClient(uri).connect().then((client) => ({ client, db: client.db("rutasur") }));
  }
  return (await globalForMongo.rutasurMongo).db;
}

export function serializar<T>(value: T): T {
  return JSON.parse(JSON.stringify(value, (_key, v) => v instanceof Date ? v.toISOString() : v));
}
