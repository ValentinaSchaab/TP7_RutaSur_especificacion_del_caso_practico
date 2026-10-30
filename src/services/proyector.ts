import { ClientSession } from "mongodb";
import { iniciarConsumidor } from "../lib/consumer";
import { Idempotencia } from "../lib/mongo";
import { GRUPOS, TOPICS } from "../lib/topics";
import { Deps, Servicio } from "./comun";

/**
 * Proyector (CQRS, lado de lectura): mantiene `envios`, `alertas` y `notificaciones` en MongoDB a partir
 * de los eventos. MongoDB es una VISTA: se puede borrar y reconstruir leyendo Kafka desde el inicio.
 * Todas las escrituras dependen solo de datos del evento (no del reloj ni del orden de llegada entre
 * topics), así la reconstrucción deja exactamente el mismo resultado.
 */
export async function iniciar(d: Deps): Promise<Servicio> {
  const { mongo, log } = d;
  const envios = mongo.db.collection<any>("envios"); // eslint-disable-line @typescript-eslint/no-explicit-any
  const alertas = mongo.db.collection<any>("alertas"); // eslint-disable-line @typescript-eslint/no-explicit-any
  const notificaciones = mongo.db.collection<any>("notificaciones"); // eslint-disable-line @typescript-eslint/no-explicit-any

  /** Upsert del documento del envío + entrada en el historial embebido (ordenado por tiempo del evento). */
  async function tocarEnvio(s: ClientSession, e: any, entrada: any, set: any = {}, inc: any = {}) { // eslint-disable-line @typescript-eslint/no-explicit-any
    const ts = new Date(e.ocurridoEn);
    const update: any = { // eslint-disable-line @typescript-eslint/no-explicit-any
      $set: { shipmentId: e.shipmentId, hubId: e.hubId, zonaId: e.zonaId, ...set },
      $min: { creadoEn: ts },
      $max: { actualizadoEn: ts },
      $push: { historial: { $each: [entrada], $sort: { ocurridoEn: 1, eventId: 1 } } },
    };
    if (Object.keys(inc).length) update.$inc = inc;
    await envios.updateOne({ _id: e.shipmentId }, update, { upsert: true, session: s });
  }

  const idem = new Idempotencia(mongo);
  const detener = await iniciarConsumidor({
    bus: d.bus, grupo: GRUPOS.PROYECTOR, idem, log, dlqSiInvalido: true,
    topics: [TOPICS.ESTADOS, TOPICS.REPROGRAMACIONES, TOPICS.ALERTAS, TOPICS.NOTIFICACIONES],
    handler: async (m, ctx) => {
      const e = m.evento;
      const s = ctx.session;

      if (m.topic === TOPICS.ESTADOS) {
        const actual = await envios.findOne({ _id: e.shipmentId }, { session: s });
        // Regla de orden: secuencia menor o igual a la ya aplicada -> se descarta.
        if (actual?.ultimaSecuencia !== undefined && e.secuencia <= actual.ultimaSecuencia) {
          log.warn(`Evento fuera de orden descartado: ${e.shipmentId} ${e.tipo} secuencia=${e.secuencia} <= ${actual.ultimaSecuencia}`);
          return;
        }
        const ts = Date.parse(e.ocurridoEn);
        const set: any = { estado: e.tipo, ultimaSecuencia: e.secuencia }; // eslint-disable-line @typescript-eslint/no-explicit-any
        if (e.repartidorId) set.repartidorId = e.repartidorId;
        if (e.promesaVenceEn) set["promesa.venceEn"] = new Date(e.promesaVenceEn);
        // "situacion": última palabra por tiempo del evento (no por orden de llegada entre topics).
        if (actual?.situacionTs === undefined || ts >= actual.situacionTs) {
          if (e.tipo === "ENTREGADO") { set.situacion = "ENTREGADO"; set.situacionTs = ts; }
          else if (e.tipo !== "ENTREGA_FALLIDA") { set.situacion = "EN_CIRCUITO"; set.situacionTs = ts; }
        }
        const entrada: any = { origen: "estados", tipo: e.tipo, ocurridoEn: e.ocurridoEn, secuencia: e.secuencia, eventId: e.eventId }; // eslint-disable-line @typescript-eslint/no-explicit-any
        if (e.motivo) entrada.motivo = e.motivo;
        if (e.intento) entrada.intento = e.intento;
        const inc = e.tipo === "ENTREGADO" || e.tipo === "ENTREGA_FALLIDA" ? { intentos: 1 } : {};
        await tocarEnvio(s, e, entrada, set, inc);
        return;
      }

      if (m.topic === TOPICS.REPROGRAMACIONES) {
        const actual = await envios.findOne({ _id: e.shipmentId }, { session: s });
        const ts = Date.parse(e.ocurridoEn);
        const set: any = { reprogramacion: { tipo: e.tipo, motivo: e.motivo, intento: e.intento, nuevoIntento: e.nuevoIntento ?? null, causa: e.causa ?? null } }; // eslint-disable-line @typescript-eslint/no-explicit-any
        if (actual?.situacionTs === undefined || ts >= actual.situacionTs) { set.situacion = e.tipo; set.situacionTs = ts; }
        const entrada: any = { origen: "reprogramaciones", tipo: e.tipo, ocurridoEn: e.ocurridoEn, eventId: e.eventId, motivo: e.motivo, intento: e.intento }; // eslint-disable-line @typescript-eslint/no-explicit-any
        if (e.nuevoIntento) entrada.nuevoIntento = e.nuevoIntento;
        if (e.causa) entrada.causa = e.causa;
        await tocarEnvio(s, e, entrada, set);
        return;
      }

      if (m.topic === TOPICS.ALERTAS) {
        const id = `${e.shipmentId}:${e.regla}`;
        const previa = await alertas.findOne({ _id: id }, { session: s });
        const base = { shipmentId: e.shipmentId, regla: e.regla, prioridad: e.prioridad, hubId: e.hubId, zonaId: e.zonaId };
        if (e.tipo === "ALERTA_ABIERTA") {
          if (previa?.estado === "CERRADA") return; // la alerta ya se cerró (llegó fuera de orden)
          await alertas.updateOne({ _id: id }, { $set: { ...base, estado: "ABIERTA", causa: e.causa, abiertaEn: new Date(e.ocurridoEn) } }, { upsert: true, session: s });
        } else {
          await alertas.updateOne({ _id: id }, { $set: { ...base, estado: "CERRADA", cerradaEn: new Date(e.ocurridoEn) }, $setOnInsert: { causa: e.causa } }, { upsert: true, session: s });
        }
        await tocarEnvio(s, e, { origen: "alertas", tipo: e.tipo, ocurridoEn: e.ocurridoEn, eventId: e.eventId, regla: e.regla, prioridad: e.prioridad, causa: e.causa });
        return;
      }

      if (m.topic === TOPICS.NOTIFICACIONES) {
        await notificaciones.updateOne(
          { _id: e.eventId },
          { $setOnInsert: { shipmentId: e.shipmentId, tipo: e.tipo, canal: e.canal, mensaje: e.mensaje, opciones: e.opciones ?? [], enviadaEn: new Date(e.ocurridoEn), hubId: e.hubId, zonaId: e.zonaId } },
          { upsert: true, session: s },
        );
        await tocarEnvio(s, e, { origen: "notificaciones", tipo: e.tipo, ocurridoEn: e.ocurridoEn, eventId: e.eventId, canal: e.canal });
      }
    },
  });
  return { detener };
}
