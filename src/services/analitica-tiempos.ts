import { iniciarConsumidor } from "../lib/consumer";
import { Idempotencia } from "../lib/mongo";
import { fechaDe, franjaDe } from "../lib/tiempo";
import { GRUPOS, TOPICS } from "../lib/topics";
import { Deps, Servicio } from "./comun";

/**
 * CUARTO INDICADOR (demo de reprocesamiento): tiempo medio entre INGRESADO_HUB y EN_REPARTO por hub.
 * Es un grupo de consumidores NUEVO que lee envios.estados desde el inicio y completa el histórico
 * (tiempoHub.{cantidad,sumaMs} en metricas_diarias) sin tocar a los demás servicios.
 */
export async function iniciar(d: Deps): Promise<Servicio> {
  const col = d.mongo.db.collection<any>("metricas_diarias"); // eslint-disable-line @typescript-eslint/no-explicit-any
  const estado = d.mongo.db.collection<any>("tiempos_estado"); // eslint-disable-line @typescript-eslint/no-explicit-any
  const idem = new Idempotencia(d.mongo);
  const detener = await iniciarConsumidor({
    bus: d.bus, grupo: GRUPOS.ANALITICA_TIEMPOS, idem, log: d.log, topics: [TOPICS.ESTADOS], fromBeginning: true,
    handler: async (m, ctx) => {
      const e = m.evento;
      if (e.tipo !== "INGRESADO_HUB" && e.tipo !== "EN_REPARTO") return;
      const s = ctx.session;
      const ts = Date.parse(e.ocurridoEn);
      let doc = (await estado.findOne({ _id: e.shipmentId }, { session: s })) ?? { _id: e.shipmentId, contado: false };
      if (e.tipo === "INGRESADO_HUB" && doc.ingresadoEn === undefined) doc.ingresadoEn = ts;
      // Solo cuenta la primera salida a reparto (los reintentos no vuelven a pasar por el hub).
      if (e.tipo === "EN_REPARTO" && doc.enRepartoEn === undefined) {
        doc.enRepartoEn = ts;
        doc.dims = { fecha: fechaDe(ts), franja: franjaDe(ts), zonaId: e.zonaId, hubId: e.hubId };
      }
      if (doc.ingresadoEn !== undefined && doc.enRepartoEn !== undefined && !doc.contado) {
        doc.contado = true;
        await col.updateOne(
          { fecha: doc.dims.fecha, zonaId: doc.dims.zonaId, franja: doc.dims.franja },
          { $inc: { "tiempoHub.cantidad": 1, "tiempoHub.sumaMs": doc.enRepartoEn - doc.ingresadoEn }, $setOnInsert: { hubId: doc.dims.hubId } },
          { upsert: true, session: s },
        );
      }
      await estado.replaceOne({ _id: e.shipmentId }, doc, { upsert: true, session: s });
    },
  });
  return { detener };
}
