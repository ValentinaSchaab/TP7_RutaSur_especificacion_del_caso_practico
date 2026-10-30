import { createHash } from "crypto";
import { Franja } from "./schemas";

export const MIN = 60_000;
export const HORA = 3_600_000;
export const DIA = 86_400_000;

/** Fecha UTC (YYYY-MM-DD) de un instante. El tiempo simulado se maneja siempre en UTC. */
export const fechaDe = (ms: number | string): string => new Date(ms).toISOString().slice(0, 10);
/** Franja horaria: antes de las 13:00 es MANANA, después TARDE. */
export const franjaDe = (ms: number | string): Franja => (new Date(ms).getUTCHours() < 13 ? "MANANA" : "TARDE");
export const franjaOpuesta = (f: Franja): Franja => (f === "MANANA" ? "TARDE" : "MANANA");
const inicioDeDia = (ms: number) => Math.floor(ms / DIA) * DIA;
export const esHabil = (ms: number): boolean => ![0, 6].includes(new Date(ms).getUTCDay());
/** Medianoche (UTC) del próximo día hábil, estrictamente posterior al día de `ms`. */
export function proximoDiaHabil(ms: number): number {
  let d = inicioDeDia(ms) + DIA;
  while (!esHabil(d)) d += DIA;
  return d;
}

/** UUID determinístico a partir de un texto: mismo texto, mismo id (clave para la idempotencia). */
export function idDeterministico(texto: string): string {
  const h = createHash("sha256").update(texto).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

/**
 * Reloj simulado. El monitor de SLA NO usa el reloj de la máquina: el tiempo "viaja en los
 * eventos" (observe). Entre eventos avanza con el factor de aceleración, así detecta la
 * AUSENCIA de eventos aunque el flujo se detenga.
 */
export class RelojSim {
  private base = 0;
  private desde = Date.now();
  constructor(private readonly factor: number) {}
  restaurar(simMs: number) {
    this.base = simMs;
    this.desde = Date.now();
  }
  observar(simMs: number) {
    if (simMs > this.now()) this.restaurar(simMs);
  }
  now(): number {
    return this.base === 0 ? 0 : this.base + (Date.now() - this.desde) * this.factor;
  }
  /** Para pruebas: fija el reloj sin extrapolar. */
  fijar(simMs: number) {
    this.restaurar(simMs);
  }
}
