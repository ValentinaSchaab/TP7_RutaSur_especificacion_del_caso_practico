import { iniciarConsumidor } from "../lib/consumer";
import { Idempotencia } from "../lib/mongo";
import { MOTIVOS } from "../lib/schemas";
import { fechaDe, franjaDe } from "../lib/tiempo";
import { GRUPOS, TOPICS } from "../lib/topics";
import { Deps, Servicio } from "./comun";

/**
 * Analítica: convierte los resultados de entrega en agregados incrementales de `metricas_diarias`
 * (un documento por fecha + zona + franja, actualizado con $inc atómicos; nunca se recalcula nada).
 *  1. Tasa de entrega en primer intento -> primerIntento.{total,exitosos}
 *  2. Distribución de motivos de fallo  -> motivos.<MOTIVO>
 *  3. Cumplimiento de SLA por hub       -> sla.{entregados,aTiempo}
 */
export async function iniciar(d: Deps): Promise<Servicio> {
  const col = d.mongo.db.collection<any>("metricas_diarias"); // eslint-disable-line @typescript-eslint/no-explicit-any
  const idem = new Idempotencia(d.mongo);
  const detener = await iniciarConsumidor({
    bus: d.bus, grupo: GRUPOS.ANALITICA, idem, log: d.log, topics: [TOPICS.ESTADOS],
    handler: async (m, ctx) => {
      const e = m.evento;
      if (e.tipo !== "ENTREGADO" && e.tipo !== "ENTREGA_FALLIDA") return;
      const inc: Record<string, number> = { "primerIntento.total": 0, "primerIntento.exitosos": 0, "sla.entregados": 0, "sla.aTiempo": 0 };
      for (const mo of MOTIVOS) inc[`motivos.${mo}`] = 0;
      if (e.intento === 1) {
        inc["primerIntento.total"] = 1;
        if (e.tipo === "ENTREGADO") inc["primerIntento.exitosos"] = 1;
      }
      if (e.tipo === "ENTREGA_FALLIDA") inc[`motivos.${e.motivo}`] = 1;
      if (e.tipo === "ENTREGADO") {
        inc["sla.entregados"] = 1;
        if (e.promesaVenceEn && Date.parse(e.ocurridoEn) <= Date.parse(e.promesaVenceEn)) inc["sla.aTiempo"] = 1;
      }
      await col.updateOne(
        { fecha: fechaDe(e.ocurridoEn), zonaId: e.zonaId, franja: franjaDe(e.ocurridoEn) },
        { $inc: inc, $setOnInsert: { hubId: e.hubId } },
        { upsert: true, session: ctx.session },
      );
    },
  });
  return { detener };
}
