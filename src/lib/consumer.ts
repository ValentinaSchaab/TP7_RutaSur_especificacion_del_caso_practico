import { ClientSession } from "mongodb";
import { Bus, BusMessage } from "./bus";
import { Idempotencia } from "./mongo";
import { Logger } from "./log";
import { SCHEMA_POR_TOPIC } from "./schemas";
import { TOPICS } from "./topics";

export interface Mensaje {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  evento: any; // ya validado contra el esquema del topic
}
export interface Ctx {
  /** Sesión de la transacción de Mongo en la que se registró el eventId. */
  session: ClientSession;
  /** Ejecuta `cb` solo después del commit de la transacción. */
  despuesDelCommit(cb: () => void | Promise<void>): void;
}

export interface OpcionesConsumidor {
  bus: Bus;
  grupo: string;
  topics: string[];
  idem: Idempotencia;
  log: Logger;
  handler: (m: Mensaje, ctx: Ctx) => Promise<void>;
  /** true: un evento que no cumple el esquema va a la DLQ. false: se registra y se omite (para no multiplicar copias). */
  dlqSiInvalido?: boolean;
  fromBeginning?: boolean;
  /** Esperas entre reintentos. Por defecto 1 s, 2 s, 4 s (3 reintentos). */
  backoffMs?: number[];
}

const dormir = (ms: number) => new Promise((r) => setTimeout(r, ms));
function backoffPorDefecto(): number[] {
  const env = process.env.RETRY_BACKOFF_MS;
  return env ? env.split(",").map((n) => Number(n.trim())) : [1000, 2000, 4000];
}

export async function iniciarConsumidor(o: OpcionesConsumidor): Promise<() => Promise<void>> {
  const detener = await o.bus.subscribe({ groupId: o.grupo, topics: o.topics, fromBeginning: o.fromBeginning, handler: (m) => procesar(o, m) });
  o.log.info(`Escuchando ${o.topics.join(", ")} (grupo "${o.grupo}"${o.fromBeginning ? ", desde el inicio" : ""})`);
  return detener;
}

/**
 * Procesa un mensaje:
 *  1. Valida el esquema. Inválido -> DLQ con el motivo (o se omite) y el consumo continúa.
 *  2. Aplica el efecto con idempotencia transaccional. Error técnico -> 3 reintentos con espera
 *     creciente; si sigue fallando, DLQ. Así un mensaje roto no frena la partición.
 *  3. Solo lanza si no pudo escribir en la DLQ (el mensaje se vuelve a entregar; el offset no se confirma).
 */
export async function procesar(o: OpcionesConsumidor, msg: BusMessage): Promise<void> {
  const schema = SCHEMA_POR_TOPIC[msg.topic];
  let evento: unknown;
  try {
    const json = JSON.parse(msg.value);
    const r = schema ? schema.safeParse(json) : { success: true as const, data: json };
    if (!r.success) throw new Error("esquema inválido: " + r.error.issues.map((i) => `${i.path.join(".") || "(raíz)"}: ${i.message}`).join("; "));
    evento = r.data;
  } catch (err) {
    const motivo = err instanceof SyntaxError ? `JSON inválido: ${err.message}` : (err as Error).message;
    if (o.dlqSiInvalido) {
      o.log.error(`Evento inválido en ${msg.topic} p${msg.partition} @${msg.offset} -> DLQ (${motivo})`);
      await enviarADlq(o, msg, motivo);
    } else {
      o.log.warn(`Evento inválido en ${msg.topic} p${msg.partition} @${msg.offset}: se omite (${motivo})`);
    }
    return;
  }

  const ev = evento as { eventId: string; shipmentId: string; tipo?: string };
  o.log.info(`${msg.topic} p${msg.partition} @${msg.offset} eventId=${ev.eventId} ${ev.tipo ?? ""} ${ev.shipmentId}`);

  const espera = o.backoffMs ?? backoffPorDefecto();
  let ultimo = new Error("desconocido");
  for (let intento = 0; intento <= espera.length; intento++) {
    try {
      const despues: Array<() => void | Promise<void>> = [];
      const res = await o.idem.runOnce(o.grupo, ev.eventId, async (session) => {
        despues.length = 0; // si la transacción se reintenta, no acumular callbacks viejos
        await o.handler({ topic: msg.topic, partition: msg.partition, offset: msg.offset, key: msg.key, evento }, { session, despuesDelCommit: (cb) => void despues.push(cb) });
      });
      if (res === "duplicado") o.log.warn(`Evento duplicado descartado: eventId=${ev.eventId}`);
      else for (const cb of despues) await cb();
      return;
    } catch (err) {
      ultimo = err as Error;
      if (intento < espera.length) {
        o.log.warn(`Falló eventId=${ev.eventId} (intento ${intento + 1}/${espera.length + 1}): ${ultimo.message}. Reintento en ${espera[intento]} ms`);
        await dormir(espera[intento]);
      }
    }
  }
  o.log.error(`eventId=${ev.eventId} falló ${espera.length + 1} veces -> DLQ. Motivo: ${ultimo.message}`);
  await enviarADlq(o, msg, ultimo.message);
}

async function enviarADlq(o: OpcionesConsumidor, msg: BusMessage, motivo: string): Promise<void> {
  await o.bus.publish({
    topic: TOPICS.DLQ,
    key: msg.key,
    value: msg.value, // el mensaje original, intacto
    headers: {
      "x-error": motivo.slice(0, 500),
      "x-topic-origen": msg.topic,
      "x-particion": String(msg.partition),
      "x-offset": msg.offset,
      "x-grupo": o.grupo,
      "x-fallo-en": new Date().toISOString(),
    },
  });
}
