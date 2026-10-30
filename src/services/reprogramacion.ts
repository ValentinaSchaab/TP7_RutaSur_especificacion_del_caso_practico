import { iniciarConsumidor } from "../lib/consumer";
import { Idempotencia } from "../lib/mongo";
import { fechaDe, franjaDe, franjaOpuesta, idDeterministico, proximoDiaHabil } from "../lib/tiempo";
import { GRUPOS, TOPICS } from "../lib/topics";
import { Deps, Servicio, publicar } from "./comun";

/**
 * Reprogramación: decide el próximo intento tras una entrega fallida.
 *  - Tercer intento fallido (cualquier motivo): devolución al comercio.
 *  - AUSENTE: día hábil siguiente, en la franja opuesta a la del fallo.
 *  - DIRECCION_INCORRECTA: queda retenido hasta una corrección; no se reprograma solo.
 *  - RECHAZADO: se marca para devolución al comercio.
 *  - ZONA_INACCESIBLE (el documento no la define): se asume día hábil siguiente, misma franja.
 */
export async function iniciar(d: Deps): Promise<Servicio> {
  const idem = new Idempotencia(d.mongo);
  const detener = await iniciarConsumidor({
    bus: d.bus, grupo: GRUPOS.REPROGRAMACION, idem, log: d.log,
    topics: [TOPICS.ESTADOS],
    handler: async (m) => {
      const e = m.evento;
      if (e.tipo !== "ENTREGA_FALLIDA") return;
      const ms = Date.parse(e.ocurridoEn);
      const base = { eventId: idDeterministico(`reprogramacion:${e.eventId}`), shipmentId: e.shipmentId, motivo: e.motivo, intento: e.intento, ocurridoEn: e.ocurridoEn, version: 1, hubId: e.hubId, zonaId: e.zonaId };
      let decision: Record<string, unknown>;
      if (e.intento >= 3) decision = { tipo: "DEVOLUCION", causa: "TERCER_FALLO" };
      else if (e.motivo === "AUSENTE") decision = { tipo: "REPROGRAMADO", nuevoIntento: { fecha: fechaDe(proximoDiaHabil(ms)), franja: franjaOpuesta(franjaDe(ms)) } };
      else if (e.motivo === "ZONA_INACCESIBLE") decision = { tipo: "REPROGRAMADO", nuevoIntento: { fecha: fechaDe(proximoDiaHabil(ms)), franja: franjaDe(ms) } };
      else if (e.motivo === "DIRECCION_INCORRECTA") decision = { tipo: "RETENIDO", causa: "DIRECCION_INCORRECTA" };
      else decision = { tipo: "DEVOLUCION", causa: "RECHAZADO" };
      await publicar(d.bus, TOPICS.REPROGRAMACIONES, { ...base, ...decision });
    },
  });
  return { detener };
}
