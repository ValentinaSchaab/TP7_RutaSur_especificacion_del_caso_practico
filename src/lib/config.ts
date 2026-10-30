/** Configuración leída de variables de entorno (valores por defecto para correr fuera de Docker). */
export const config = {
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  // directConnection=true: el replica set se anuncia como "mongo:27017", que desde tu máquina no resuelve.
  mongoUri: process.env.MONGO_URI ?? "mongodb://localhost:27017/rutasur?directConnection=true",
  /** 1 hora simulada = 3600/factor segundos reales. 120 => 1 h simulada = 30 s. */
  factor: Number(process.env.SIM_FACTOR ?? 120),
  seed: Number(process.env.SIM_SEED ?? 42),
  enviosPorMinuto: Number(process.env.SIM_ENVIOS_POR_MINUTO ?? 20),
  autoStart: (process.env.SIM_AUTOSTART ?? "true") === "true",
  /** Arranque del reloj simulado (un lunes). */
  simInicio: process.env.SIM_INICIO ?? "2026-11-02T06:00:00.000Z",
  simPort: Number(process.env.SIM_PORT ?? 4000),
  /** El monitor de SLA evalúa todos los envíos abiertos cada este tiempo (ms reales). El documento pide 1 minuto. */
  slaTickMs: Number(process.env.SLA_TICK_MS ?? 60_000),
  prob: {
    demoraHub: Number(process.env.SIM_P_DEMORA_HUB ?? 0.1),
    duplicado: Number(process.env.SIM_P_DUPLICADO ?? 0.02),
    fueraDeOrden: Number(process.env.SIM_P_FUERA_DE_ORDEN ?? 0.02),
    malformado: Number(process.env.SIM_P_MALFORMADO ?? 0.01),
  },
};
