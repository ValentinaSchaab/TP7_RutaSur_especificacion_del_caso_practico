import { ClientSession, Db, MongoClient } from "mongodb";

export interface Mongo {
  client: MongoClient;
  db: Db;
}

export async function conectar(uri: string): Promise<Mongo> {
  const client = new MongoClient(uri);
  await client.connect();
  return { client, db: client.db() };
}

const SIETE_DIAS_S = 7 * 24 * 3600;

/**
 * Crea colecciones e índices del modelo de lectura. Las transacciones de MongoDB no pueden crear
 * colecciones nuevas, por eso se crean al arrancar. Es idempotente: todos los servicios lo llaman.
 */
export async function prepararBase(db: Db): Promise<void> {
  const colecciones = ["envios", "alertas", "notificaciones", "metricas_diarias", "eventos_procesados", "sla_estado", "sla_emitidas", "sla_reloj", "tiempos_estado"];
  for (const c of colecciones) {
    try { await db.createCollection(c); } catch (e) { if ((e as { code?: number }).code !== 48) throw e; }
  }
  await db.collection("envios").createIndexes([
    { key: { shipmentId: 1 }, unique: true },
    { key: { estado: 1, zonaId: 1 } },
    { key: { "promesa.venceEn": 1 } },
  ]);
  await db.collection("alertas").createIndexes([{ key: { estado: 1, prioridad: 1 } }, { key: { shipmentId: 1 } }]);
  await db.collection("notificaciones").createIndex({ shipmentId: 1 });
  await db.collection("metricas_diarias").createIndex({ fecha: 1, zonaId: 1, franja: 1 }, { unique: true });
  await db.collection("eventos_procesados").createIndexes([
    { key: { consumidor: 1, eventId: 1 }, unique: true },
    { key: { en: 1 }, expireAfterSeconds: SIETE_DIAS_S }, // igual que la retención de los topics cortos
  ]);
  await db.collection("sla_emitidas").createIndex({ shipmentId: 1, regla: 1 }, { unique: true });
}

/**
 * Idempotencia: el eventId se registra en `eventos_procesados` EN LA MISMA TRANSACCIÓN que el efecto.
 * Si el efecto falla, el registro tampoco queda y el reintento vuelve a procesar. Si ya estaba, se descarta.
 */
export class Idempotencia {
  constructor(private readonly m: Mongo) {}

  async runOnce(consumidor: string, eventId: string, trabajo: (s: ClientSession) => Promise<void>): Promise<"procesado" | "duplicado"> {
    const col = this.m.db.collection<{ consumidor: string; eventId: string; en: Date }>("eventos_procesados");
    const session = this.m.client.startSession();
    try {
      let duplicado = false;
      await session.withTransaction(async () => {
        duplicado = false;
        if (await col.findOne({ consumidor, eventId }, { session })) { duplicado = true; return; }
        await col.insertOne({ consumidor, eventId, en: new Date() }, { session });
        await trabajo(session);
      });
      return duplicado ? "duplicado" : "procesado";
    } finally {
      await session.endSession();
    }
  }
}
