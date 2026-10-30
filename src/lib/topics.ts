export const TOPICS = {
  ESTADOS: "envios.estados",
  PROMESAS: "envios.promesas",
  REPROGRAMACIONES: "envios.reprogramaciones",
  ALERTAS: "alertas.sla",
  NOTIFICACIONES: "notificaciones.enviadas",
  DLQ: "envios.estados.dlq",
} as const;

/** Consumer groups (uno por servicio, según el documento). */
export const GRUPOS = {
  PROYECTOR: "proyector",
  NOTIFICACIONES: "notificaciones",
  REPROGRAMACION: "reprogramacion",
  MONITOR_SLA: "monitor-sla",
  ANALITICA: "analitica",
  ANALITICA_TIEMPOS: "analitica-tiempos",
} as const;
