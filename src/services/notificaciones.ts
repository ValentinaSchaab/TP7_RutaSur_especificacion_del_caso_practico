import { iniciarConsumidor } from "../lib/consumer";
import { Idempotencia } from "../lib/mongo";
import { Motivo } from "../lib/schemas";
import { idDeterministico } from "../lib/tiempo";
import { GRUPOS, TOPICS } from "../lib/topics";
import { Deps, Servicio, publicar } from "./comun";

const MENSAJE: Record<Motivo, string> = {
  AUSENTE: "No pudimos entregar tu paquete porque no había nadie en el domicilio.",
  DIRECCION_INCORRECTA: "No pudimos entregar tu paquete: la dirección parece incorrecta. Necesitamos que la confirmes.",
  RECHAZADO: "El paquete fue rechazado en la entrega y se devolverá al comercio.",
  ZONA_INACCESIBLE: "No pudimos acceder a tu zona. Vamos a intentarlo nuevamente.",
};

/**
 * Notificaciones: registra avisos al destinatario ante fallos y alertas (no se envía correo real; el
 * aviso queda registrado en notificaciones.enviadas y el proyector lo refleja en MongoDB).
 */
export async function iniciar(d: Deps): Promise<Servicio> {
  const idem = new Idempotencia(d.mongo);
  const detener = await iniciarConsumidor({
    bus: d.bus, grupo: GRUPOS.NOTIFICACIONES, idem, log: d.log,
    topics: [TOPICS.ESTADOS, TOPICS.ALERTAS],
    handler: async (m) => {
      const e = m.evento;
      if (m.topic === TOPICS.ESTADOS && e.tipo === "ENTREGA_FALLIDA") {
        const reprograma = e.intento < 3 && (e.motivo === "AUSENTE" || e.motivo === "ZONA_INACCESIBLE");
        const opciones = reprograma ? ["REPROGRAMAR_MANANA", "REPROGRAMAR_TARDE"] : e.motivo === "DIRECCION_INCORRECTA" && e.intento < 3 ? ["CORREGIR_DIRECCION"] : [];
        await publicar(d.bus, TOPICS.NOTIFICACIONES, {
          eventId: idDeterministico(`notificacion:${e.eventId}`), // reprocesar el mismo evento da el mismo eventId
          shipmentId: e.shipmentId, tipo: "AVISO_FALLO", canal: "EMAIL", mensaje: MENSAJE[e.motivo as Motivo], opciones,
          ocurridoEn: e.ocurridoEn, version: 1, hubId: e.hubId, zonaId: e.zonaId,
        });
      } else if (m.topic === TOPICS.ALERTAS && e.tipo === "ALERTA_ABIERTA" && (e.prioridad === "ALTA" || e.prioridad === "CRITICA")) {
        await publicar(d.bus, TOPICS.NOTIFICACIONES, {
          eventId: idDeterministico(`notificacion:${e.eventId}`),
          shipmentId: e.shipmentId, tipo: "AVISO_ALERTA", canal: "EMAIL",
          mensaje: `Tu envío puede llegar con demora (${e.causa}). Estamos trabajando para resolverlo.`,
          ocurridoEn: e.ocurridoEn, version: 1, hubId: e.hubId, zonaId: e.zonaId,
        });
      }
    },
  });
  return { detener };
}
