import { Consumer, Kafka, Partitioners, Producer, logLevel } from "kafkajs";

/**
 * Abstracción mínima del bus de mensajes: el resto del sistema habla con esta interfaz y solo
 * KafkaBus conoce kafkajs (si hay que reemplazarlo, se cambia SOLO esa clase).
 */
export interface BusMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}
export interface PublishInput {
  topic: string;
  key: string | null;
  value: string;
  headers?: Record<string, string>;
}
export interface SubscribeInput {
  groupId: string;
  topics: string[];
  fromBeginning?: boolean;
  /** El offset se confirma SOLO si la promesa se resuelve (commit manual, después de procesar). */
  handler: (m: BusMessage) => Promise<void>;
}
export interface Bus {
  publish(input: PublishInput): Promise<void>;
  /** Devuelve una función que detiene este consumidor (el offset confirmado se conserva). */
  subscribe(input: SubscribeInput): Promise<() => Promise<void>>;
  disconnect(): Promise<void>;
}

export class KafkaBus implements Bus {
  private readonly kafka: Kafka;
  private producer?: Producer;
  private producerReady?: Promise<void>;
  private readonly consumers: Consumer[] = [];

  constructor(brokers: string[], clientId: string) {
    this.kafka = new Kafka({ clientId, brokers, logLevel: logLevel.WARN, retry: { initialRetryTime: 300, retries: 10 } });
  }

  private async getProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner, allowAutoTopicCreation: false });
      this.producerReady = this.producer.connect();
    }
    await this.producerReady;
    return this.producer;
  }

  async publish(i: PublishInput): Promise<void> {
    const p = await this.getProducer();
    await p.send({ topic: i.topic, messages: [{ key: i.key, value: i.value, headers: i.headers }] });
  }

  async subscribe(i: SubscribeInput): Promise<() => Promise<void>> {
    const consumer = this.kafka.consumer({ groupId: i.groupId, sessionTimeout: 15000, heartbeatInterval: 3000, allowAutoTopicCreation: false });
    this.consumers.push(consumer);
    await consumer.connect();
    for (const topic of i.topics) await consumer.subscribe({ topic, fromBeginning: i.fromBeginning ?? false });
    await consumer.run({
      autoCommit: false, // commit manual: después de escribir en MongoDB, nunca antes
      eachMessage: async ({ topic, partition, message }) => {
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) if (v !== undefined) headers[k] = Array.isArray(v) ? v.map(String).join(",") : v.toString();
        await i.handler({
          topic, partition, offset: message.offset,
          key: message.key ? message.key.toString() : null,
          value: message.value ? message.value.toString() : "",
          headers,
        });
        await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
      },
    });
    return async () => { await consumer.disconnect().catch(() => undefined); };
  }

  async disconnect(): Promise<void> {
    for (const c of this.consumers) await c.disconnect().catch(() => undefined);
    await this.producer?.disconnect().catch(() => undefined);
  }
}

interface SubMem {
  groupId: string; topics: Set<string>; handler: (m: BusMessage) => Promise<void>;
  draining: boolean; again: boolean; activa: boolean;
}

/** Bus en memoria con la misma semántica básica de Kafka (particiones, offsets por grupo, replay). Solo para pruebas. */
export class InMemoryBus implements Bus {
  private readonly partitions = 3;
  private readonly logs = new Map<string, BusMessage[][]>();
  private readonly offsets = new Map<string, number>(); // "grupo|topic|partición" -> próximo índice (persiste si el consumidor se detiene)
  private readonly subs: SubMem[] = [];
  private activos = 0;

  private log(topic: string): BusMessage[][] {
    let l = this.logs.get(topic);
    if (!l) { l = Array.from({ length: this.partitions }, () => []); this.logs.set(topic, l); }
    return l;
  }
  private particion(key: string | null): number {
    if (!key) return 0;
    let h = 0;
    for (const c of key) h = (h * 31 + c.charCodeAt(0)) >>> 0;
    return h % this.partitions;
  }
  mensajes(topic: string): BusMessage[] { return this.log(topic).flat(); }

  async publish(i: PublishInput): Promise<void> {
    const partition = this.particion(i.key);
    const p = this.log(i.topic)[partition];
    p.push({ topic: i.topic, partition, offset: String(p.length), key: i.key, value: i.value, headers: i.headers ?? {} });
    this.kick();
  }

  async subscribe(i: SubscribeInput): Promise<() => Promise<void>> {
    const sub: SubMem = { groupId: i.groupId, topics: new Set(i.topics), handler: i.handler, draining: false, again: false, activa: true };
    for (const t of i.topics)
      this.log(t).forEach((part, idx) => {
        const k = `${i.groupId}|${t}|${idx}`;
        if (!this.offsets.has(k)) this.offsets.set(k, i.fromBeginning ? 0 : part.length);
      });
    this.subs.push(sub);
    this.kick();
    return async () => { sub.activa = false; while (sub.draining) await new Promise((r) => setTimeout(r, 5)); };
  }

  private kick() {
    for (const s of this.subs) {
      if (!s.activa) continue;
      if (s.draining) { s.again = true; continue; }
      s.draining = true; this.activos++;
      void this.drain(s);
    }
  }
  private async drain(s: SubMem) {
    try {
      do {
        s.again = false;
        for (const t of s.topics) {
          const parts = this.log(t);
          for (let i = 0; i < parts.length; i++) {
            const k = `${s.groupId}|${t}|${i}`;
            let n = this.offsets.get(k) ?? 0;
            while (s.activa && n < parts[i].length) {
              try { await s.handler(parts[i][n]); n++; this.offsets.set(k, n); }
              catch { await new Promise((r) => setTimeout(r, 20)); } // como kafkajs: reintenta
            }
          }
        }
      } while (s.again && s.activa);
    } finally { s.draining = false; this.activos--; }
  }

  /** Como `kafka-consumer-groups --reset-offsets --to-earliest`: el grupo vuelve a leer desde el inicio (con el consumidor detenido). */
  retrocederGrupo(groupId: string): void {
    for (const k of [...this.offsets.keys()]) if (k.startsWith(`${groupId}|`)) this.offsets.set(k, 0);
  }

  /** Espera a que todos los consumidores terminen de procesar lo publicado. */
  async cuandoEsteQuieto(quietoMs = 80): Promise<void> {
    let quieto = 0;
    while (quieto < quietoMs) { await new Promise((r) => setTimeout(r, 10)); quieto = this.activos === 0 ? quieto + 10 : 0; }
  }
  async disconnect(): Promise<void> { for (const s of this.subs) s.activa = false; }
}
