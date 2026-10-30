import { z } from "zod";
import { TOPICS } from "./topics";

export const MOTIVOS = ["AUSENTE", "DIRECCION_INCORRECTA", "RECHAZADO", "ZONA_INACCESIBLE"] as const;
export const TIPOS_ESTADO = ["CREADO", "INGRESADO_HUB", "CLASIFICADO", "EN_REPARTO", "ENTREGADO", "ENTREGA_FALLIDA"] as const;
export const REGLAS_SLA = ["SIN_INGRESO", "SIN_SALIDA", "FALLO_DIA_LIMITE", "VENCIDO"] as const;
export const PRIORIDADES = ["MEDIA", "ALTA", "CRITICA"] as const;
export type Motivo = (typeof MOTIVOS)[number];
export type Franja = "MANANA" | "TARDE";

const iso = z.string().datetime();
/** Campos comunes a todo evento (sección "Campos comunes" del documento). */
const comunes = z.object({
  eventId: z.string().min(8),
  shipmentId: z.string().min(1),
  ocurridoEn: iso, // momento en que pasó en el mundo real (tiempo SIMULADO)
  version: z.number().int().positive(),
  hubId: z.string().min(1),
  zonaId: z.string().min(1),
});

/** envios.estados */
export const estadoSchema = comunes
  .extend({
    tipo: z.enum(TIPOS_ESTADO),
    secuencia: z.number().int().positive(), // creciente por envío: detecta eventos fuera de orden
    repartidorId: z.string().optional(),
    motivo: z.enum(MOTIVOS).optional(),
    intento: z.number().int().min(1).max(3).optional(),
    /** Vencimiento de la promesa (desnormalizado para no tener que cruzar topics). */
    promesaVenceEn: iso.optional(),
  })
  .superRefine((e, ctx) => {
    if (e.tipo === "ENTREGA_FALLIDA" && !e.motivo)
      ctx.addIssue({ code: "custom", path: ["motivo"], message: "ENTREGA_FALLIDA requiere motivo" });
    if ((e.tipo === "ENTREGA_FALLIDA" || e.tipo === "ENTREGADO") && !e.intento)
      ctx.addIssue({ code: "custom", path: ["intento"], message: `${e.tipo} requiere intento` });
  });

/** envios.promesas (topic compactado: queda la última promesa de cada envío) */
export const promesaSchema = comunes.extend({ venceEn: iso });

/** envios.reprogramaciones */
export const reprogramacionSchema = comunes.extend({
  tipo: z.enum(["REPROGRAMADO", "RETENIDO", "DEVOLUCION"]),
  motivo: z.enum(MOTIVOS),
  intento: z.number().int().min(1),
  nuevoIntento: z.object({ fecha: z.string(), franja: z.enum(["MANANA", "TARDE"]) }).optional(),
  causa: z.string().optional(),
});

/** alertas.sla */
export const alertaSchema = comunes.extend({
  tipo: z.enum(["ALERTA_ABIERTA", "ALERTA_CERRADA"]),
  regla: z.enum(REGLAS_SLA),
  prioridad: z.enum(PRIORIDADES),
  causa: z.string(),
});

/** notificaciones.enviadas */
export const notificacionSchema = comunes.extend({
  tipo: z.enum(["AVISO_FALLO", "AVISO_ALERTA"]),
  canal: z.literal("EMAIL"),
  mensaje: z.string(),
  opciones: z.array(z.string()).optional(),
});

export type Estado = z.infer<typeof estadoSchema>;
export type Promesa = z.infer<typeof promesaSchema>;
export type Reprogramacion = z.infer<typeof reprogramacionSchema>;
export type Alerta = z.infer<typeof alertaSchema>;
export type Notificacion = z.infer<typeof notificacionSchema>;

/** Esquema de cada topic: cada consumidor valida lo que recibe. */
export const SCHEMA_POR_TOPIC: Record<string, z.ZodTypeAny> = {
  [TOPICS.ESTADOS]: estadoSchema,
  [TOPICS.PROMESAS]: promesaSchema,
  [TOPICS.REPROGRAMACIONES]: reprogramacionSchema,
  [TOPICS.ALERTAS]: alertaSchema,
  [TOPICS.NOTIFICACIONES]: notificacionSchema,
};
