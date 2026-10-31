export type Envio = {
  _id: string; shipmentId?: string; estado?: string; situacion?: string;
  hubId?: string; zonaId?: string; repartidorId?: string; intentos?: number;
  creadoEn?: string; actualizadoEn?: string; promesa?: { venceEn?: string };
  historial?: Array<{ origen?: string; tipo?: string; ocurridoEn?: string; secuencia?: number; motivo?: string; regla?: string; prioridad?: string; causa?: string; canal?: string; nuevoIntento?: unknown }>;
  reprogramacion?: { tipo?: string; motivo?: string; nuevoIntento?: { fecha?: string; franja?: string } };
};
export type Alerta = { _id: string; shipmentId: string; regla: string; prioridad: string; estado: string; causa?: string; abiertaEn?: string; zonaId?: string; hubId?: string };
