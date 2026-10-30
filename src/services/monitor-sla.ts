import { ClientSession } from "mongodb";
import { config } from "../lib/config";
import { iniciarConsumidor } from "../lib/consumer";
import { Idempotencia } from "../lib/mongo";
import { PRIORIDADES, REGLAS_SLA } from "../lib/schemas";
import { HORA, RelojSim, fechaDe, idDeterministico } from "../lib/tiempo";
import { GRUPOS, TOPICS } from "../lib/topics";
import { Deps, Servicio, publicar } from "./comun";

type Regla = (typeof REGLAS_SLA)[number];
type Prioridad = (typeof PRIORIDADES)[number];

/** Estado interno por envío (una "tabla" respaldada en MongoDB: sobrevive a reinicios). */
interface EstadoSla {
  _id: string;
  shipmentId: string;
  estado: string | null;
  venceEn: number | null;
  hubId: string;
  zonaId: string;
  ultimaSecuencia: number;
  fallidoEn: number | null;
  cerrado: boolean;
}

export interface MonitorSla extends Servicio {
  /** Evalúa todos los envíos abiertos con el reloj simulado actual (se ejecuta cada minuto; público para pruebas). */
  tick(): Promise<void>;
  reloj: RelojSim;
}

/** Las 4 reglas del documento. Pura: dado el estado y el instante simulado, devuelve las que aplican. */
export function reglasAplicables(st: EstadoSla, ahora: number): Array<{ regla: Regla; prioridad: Prioridad; causa: string }> {
  const out: Array<{ regla: Regla; prioridad: Prioridad; causa: string }> = [];
  if (st.cerrado || st.estado === "ENTREGADO" || st.venceEn === null || st.estado === null) return out;
  const restante = st.venceEn - ahora;
  if (restante > 0 && restante < 10 * HORA && st.estado === "CREADO")
    out.push({ regla: "SIN_INGRESO", prioridad: "MEDIA", causa: "Faltan menos de 10 h para el vencimiento y no ingresó al hub" });
  if (restante > 0 && restante < 4 * HORA && st.estado !== "EN_REPARTO")
    out.push({ regla: "SIN_SALIDA", prioridad: "ALTA", causa: "Faltan menos de 4 h para el vencimiento y no está en reparto" });
  if (st.estado === "ENTREGA_FALLIDA" && st.fallidoEn !== null && fechaDe(st.venceEn) === fechaDe(st.fallidoEn))
    out.push({ regla: "FALLO_DIA_LIMITE", prioridad: "ALTA", causa: "Entrega fallida y la promesa vence hoy" });
  if (restante <= 0)
    out.push({ regla: "VENCIDO", prioridad: "CRITICA", causa: "Pasó el vencimiento sin ENTREGADO" });
  return out;
}

/**
 * Monitor de SLA: consumidor con estado propio (memoria + respaldo en MongoDB). Cruza dos streams
 * (envios.estados y envios.promesas) y usa el RELOJ SIMULADO que viaja en los eventos, no el de la máquina.
 * La alerta nace de la AUSENCIA de un evento: por eso, además de evaluar al recibir cada evento, revisa
 * todos los envíos abiertos periódicamente.
 */
export async function iniciar(d: Deps, opts: { reloj?: RelojSim; tickMs?: number } = {}): Promise<MonitorSla> {
  const { mongo, bus, log } = d;
  const reloj = opts.reloj ?? new RelojSim(config.factor);
  const estados = mongo.db.collection<EstadoSla>("sla_estado");
  const emitidas = mongo.db.collection<any>("sla_emitidas"); // eslint-disable-line @typescript-eslint/no-explicit-any
  const relojCol = mongo.db.collection<any>("sla_reloj"); // eslint-disable-line @typescript-eslint/no-explicit-any

  // --- Recuperar estado tras un reinicio ---
  const abiertos = new Map<string, EstadoSla>();
  for (const st of await estados.find({ cerrado: false }).toArray()) abiertos.set(st._id, st);
  const yaEmitidas = new Set<string>((await emitidas.find({}, { projection: { _id: 1 } }).toArray()).map((x: { _id: string }) => x._id));
  const guardado = await relojCol.findOne({ _id: "reloj" });
  if (guardado) reloj.restaurar(guardado.simMs);
  log.info(`Estado recuperado: ${abiertos.size} envíos abiertos, ${yaEmitidas.size} alertas ya emitidas`);

  /** Emite la alerta si esa regla todavía no alertó sobre ese envío ("sin repetición"). */
  async function emitir(s: ClientSession, st: EstadoSla, r: { regla: Regla; prioridad: Prioridad; causa: string }, ahora: number): Promise<boolean> {
    const id = `${st.shipmentId}:${r.regla}`;
    if (await emitidas.findOne({ _id: id }, { session: s })) return false;
    await emitidas.insertOne({ _id: id, shipmentId: st.shipmentId, regla: r.regla, prioridad: r.prioridad, cerrada: false, emitidaEn: ahora }, { session: s });
    // eventId determinístico: si hay que reintentar, sale el MISMO evento y el proyector lo descarta por duplicado.
    await publicar(bus, TOPICS.ALERTAS, {
      eventId: idDeterministico(`alerta:${id}:abierta`), shipmentId: st.shipmentId, tipo: "ALERTA_ABIERTA",
      regla: r.regla, prioridad: r.prioridad, causa: r.causa, ocurridoEn: new Date(ahora).toISOString(), version: 1, hubId: st.hubId, zonaId: st.zonaId,
    });
    log.warn(`ALERTA ${r.prioridad} ${r.regla} sobre ${st.shipmentId}: ${r.causa}`);
    return true;
  }

  async function evaluar(s: ClientSession, st: EstadoSla, ahora: number): Promise<string[]> {
    const nuevas: string[] = [];
    for (const r of reglasAplicables(st, ahora)) if (await emitir(s, st, r, ahora)) nuevas.push(`${st.shipmentId}:${r.regla}`);
    return nuevas;
  }

  /** Al llegar ENTREGADO: cierra las alertas abiertas de ese envío. */
  async function cerrarAlertas(s: ClientSession, st: EstadoSla, ahora: number) {
    for (const a of await emitidas.find({ shipmentId: st.shipmentId, cerrada: false }, { session: s }).toArray()) {
      await publicar(bus, TOPICS.ALERTAS, {
        eventId: idDeterministico(`alerta:${a._id}:cerrada`), shipmentId: st.shipmentId, tipo: "ALERTA_CERRADA",
        regla: a.regla, prioridad: a.prioridad, causa: "El envío fue entregado", ocurridoEn: new Date(ahora).toISOString(), version: 1, hubId: st.hubId, zonaId: st.zonaId,
      });
      await emitidas.updateOne({ _id: a._id }, { $set: { cerrada: true } }, { session: s });
      log.info(`Alerta ${a.regla} de ${st.shipmentId} CERRADA (entregado)`);
    }
  }

  const idem = new Idempotencia(mongo);
  const detenerConsumidor = await iniciarConsumidor({
    bus, grupo: GRUPOS.MONITOR_SLA, idem, log, topics: [TOPICS.ESTADOS, TOPICS.PROMESAS],
    handler: async (m, ctx) => {
      const e = m.evento;
      const s = ctx.session;
      const evMs = Date.parse(e.ocurridoEn);
      reloj.observar(evMs);
      // Copia: si la transacción se reintenta, el estado en memoria no debe haber cambiado.
      const previo = abiertos.get(e.shipmentId) ?? (await estados.findOne({ _id: e.shipmentId }, { session: s }));
      const st: EstadoSla = previo
        ? { ...previo }
        : { _id: e.shipmentId, shipmentId: e.shipmentId, estado: null, venceEn: null, hubId: e.hubId, zonaId: e.zonaId, ultimaSecuencia: 0, fallidoEn: null, cerrado: false };

      if (m.topic === TOPICS.PROMESAS) {
        st.venceEn = Date.parse(e.venceEn);
      } else {
        if (e.secuencia <= st.ultimaSecuencia) return; // fuera de orden o repetido: no retrocede el estado
        st.estado = e.tipo;
        st.ultimaSecuencia = e.secuencia;
        st.hubId = e.hubId;
        st.zonaId = e.zonaId;
        if (st.venceEn === null && e.promesaVenceEn) st.venceEn = Date.parse(e.promesaVenceEn);
        if (e.tipo === "ENTREGA_FALLIDA") st.fallidoEn = evMs;
      }

      const ahora = Math.max(reloj.now(), evMs);
      let nuevas: string[] = [];
      if (st.estado === "ENTREGADO") {
        await cerrarAlertas(s, st, ahora);
        st.cerrado = true;
      } else {
        nuevas = await evaluar(s, st, ahora);
      }
      await estados.replaceOne({ _id: st._id }, st, { upsert: true, session: s });
      await relojCol.updateOne({ _id: "reloj" }, { $max: { simMs: evMs } }, { upsert: true, session: s });
      ctx.despuesDelCommit(() => {
        if (st.cerrado) abiertos.delete(st._id);
        else abiertos.set(st._id, st);
        nuevas.forEach((n) => yaEmitidas.add(n));
      });
    },
  });

  async function tick() {
    const ahora = reloj.now();
    if (ahora === 0) return;
    for (const st of [...abiertos.values()]) {
      const pendientes = reglasAplicables(st, ahora).filter((r) => !yaEmitidas.has(`${st.shipmentId}:${r.regla}`));
      if (pendientes.length === 0) continue;
      await mongo.client.withSession((s) =>
        s.withTransaction(async () => {
          for (const r of pendientes) if (await emitir(s, st, r, ahora)) yaEmitidas.add(`${st.shipmentId}:${r.regla}`);
        }),
      );
    }
  }

  const timer = setInterval(() => void tick().catch((e) => log.error(`tick: ${(e as Error).message}`)), opts.tickMs ?? config.slaTickMs);
  timer.unref();
  return {
    reloj,
    tick,
    detener: async () => {
      clearInterval(timer);
      await detenerConsumidor();
    },
  };
}
