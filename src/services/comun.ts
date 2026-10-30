import { Bus } from "../lib/bus";
import { Logger } from "../lib/log";
import { Mongo } from "../lib/mongo";

export interface Deps {
  bus: Bus;
  mongo: Mongo;
  log: Logger;
}
export interface Servicio {
  detener(): Promise<void>;
}

/** Publica un evento; la clave del mensaje es SIEMPRE el shipmentId (orden por envío). */
export const publicar = (bus: Bus, topic: string, evento: { shipmentId: string; [campo: string]: unknown }) =>
  bus.publish({ topic, key: evento.shipmentId, value: JSON.stringify(evento) });
