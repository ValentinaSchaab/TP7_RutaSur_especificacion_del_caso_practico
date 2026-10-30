import { Estado, Franja, MOTIVOS, Motivo } from "../lib/schemas";
import { DIA, HORA, MIN, esHabil, franjaDe, franjaOpuesta, idDeterministico, proximoDiaHabil } from "../lib/tiempo";
import { TOPICS } from "../lib/topics";

/** Generador pseudoaleatorio con semilla: la misma semilla reproduce exactamente la misma corrida. */
export type Rng = () => number;
export function mulberry32(semilla: number): Rng {
  let a = semilla >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** 6 zonas en 3 centros de distribución, con tasas de fallo distintas (5% a 30%). */
export const ZONAS = [
  { id: "Z1", hub: "H1", tasa: 0.05 }, { id: "Z2", hub: "H1", tasa: 0.08 },
  { id: "Z3", hub: "H2", tasa: 0.12 }, { id: "Z4", hub: "H2", tasa: 0.18 },
  { id: "Z5", hub: "H3", tasa: 0.25 }, { id: "Z6", hub: "H3", tasa: 0.3 },
];
const REPARTIDORES_POR_ZONA = 20; // 120 repartidores en total
const VENTANA: Record<Franja, [number, number]> = { MANANA: [8 * HORA, 11 * HORA], TARDE: [13.5 * HORA, 15.5 * HORA] };

export interface Programado { at: number; topic: string; key: string; value: string }
export interface Probabilidades { demoraHub: number; duplicado: number; fueraDeOrden: number; malformado: number }

/** Guion para forzar el recorrido de un envío (escenarios de la demo y pruebas). */
export interface Guion {
  zonaId?: string;
  /** Pasos de pocos minutos (para que la demo se vea en segundos). */
  comprimido?: boolean;
  demoraHubMs?: number;
  /** Duración de la promesa desde la creación (por defecto: hasta las 20:00 del próximo día hábil). */
  promesaMs?: number;
  /** Motivos de las fallas en orden (intento 1, 2, 3). Sin fallas: se entrega al primer intento. */
  fallas?: Motivo[];
  /** Espera hasta el reintento (por defecto: próximo día hábil, franja opuesta). */
  reintentoMs?: number;
  /** Sin duplicados, desorden ni malformados. */
  limpio?: boolean;
}

const pad = (n: number, w: number) => String(n).padStart(w, "0");
const entre = (rng: Rng, a: number, b: number) => a + rng() * (b - a);

function enVentana(rng: Rng, desde: number, franja: Franja): number {
  const [a, b] = VENTANA[franja];
  let dia = Math.floor(desde / DIA) * DIA;
  if (!esHabil(dia) || desde > dia + b) dia = proximoDiaHabil(desde);
  let t = dia + a + rng() * (b - a);
  if (t < desde) t = desde + rng() * 10 * MIN;
  return Math.round(t);
}
const enDia = (rng: Rng, dia: number, franja: Franja) => Math.round(dia + VENTANA[franja][0] + rng() * (VENTANA[franja][1] - VENTANA[franja][0]));

const tasaDeFallo = (tasa: number, franja: Franja, intento: number) => Math.min(0.9, tasa * (franja === "TARDE" ? 1.3 : 0.8) * (intento > 1 ? 0.5 : 1));
function motivoAleatorio(rng: Rng): Motivo {
  const r = rng(); // 60% ausente, 20% dirección, 15% rechazo, 5% zona
  return r < 0.6 ? "AUSENTE" : r < 0.8 ? "DIRECCION_INCORRECTA" : r < 0.95 ? "RECHAZADO" : "ZONA_INACCESIBLE";
}

/** Un envío completo (promesa + todos sus estados con sus reintentos) ya con las perturbaciones aplicadas. */
export function generarEnvio(rng: Rng, semilla: number, shipmentId: string, t0: number, prob: Probabilidades, g: Guion = {}): Programado[] {
  const zi = g.zonaId ? ZONAS.findIndex((z) => z.id === g.zonaId) : Math.floor(rng() * ZONAS.length);
  const zona = ZONAS[zi];
  const repartidorId = `R${pad(zi * REPARTIDORES_POR_ZONA + 1 + Math.floor(rng() * REPARTIDORES_POR_ZONA), 3)}`;
  const comp = g.comprimido ?? false;
  const venceEn = t0 + (g.promesaMs ?? proximoDiaHabil(t0) + 20 * HORA - t0);
  let seq = 0;
  const evs: Estado[] = [];
  const mk = (tipo: Estado["tipo"], at: number, extra: Partial<Estado> = {}): Estado => {
    seq++;
    const e: Estado = {
      eventId: idDeterministico(`${semilla}|${shipmentId}|${seq}|${tipo}`), shipmentId, tipo, ocurridoEn: new Date(at).toISOString(),
      secuencia: seq, hubId: zona.hub, zonaId: zona.id, repartidorId, version: 1, promesaVenceEn: new Date(venceEn).toISOString(), ...extra,
    };
    evs.push(e);
    return e;
  };

  let t = t0;
  mk("CREADO", t);
  t += comp ? entre(rng, 5, 12) * MIN : entre(rng, 45, 150) * MIN;
  mk("INGRESADO_HUB", t);
  const demora = g.demoraHubMs ?? (rng() < prob.demoraHub ? entre(rng, 18, 30) * HORA : entre(rng, 20, 90) * MIN);
  t += comp && g.demoraHubMs === undefined ? entre(rng, 5, 12) * MIN : demora;
  mk("CLASIFICADO", t);

  let franjaPrevia: Franja | null = null;
  for (let intento = 1; intento <= 3; intento++) {
    let inicio: number;
    if (intento === 1) {
      if (comp) inicio = t + entre(rng, 5, 12) * MIN;
      else inicio = enVentana(rng, t + 30 * MIN, rng() < 0.5 ? "MANANA" : "TARDE");
    } else if (g.reintentoMs !== undefined) inicio = t + g.reintentoMs;
    else inicio = enDia(rng, proximoDiaHabil(t), franjaOpuesta(franjaPrevia as Franja)); // día hábil siguiente, franja opuesta
    mk("EN_REPARTO", inicio);
    const fin = Math.round(inicio + (comp ? entre(rng, 5, 12) : entre(rng, 30, 100)) * MIN);
    franjaPrevia = franjaDe(inicio);

    const falla = g.fallas ? intento <= g.fallas.length : rng() < tasaDeFallo(zona.tasa, franjaPrevia, intento);
    if (!falla) { mk("ENTREGADO", fin, { intento }); break; }
    const motivo = g.fallas ? g.fallas[intento - 1] : motivoAleatorio(rng);
    mk("ENTREGA_FALLIDA", fin, { intento, motivo });
    // Tercer fallo, dirección incorrecta (retenido) o rechazo (devolución): el envío sale del circuito.
    if (intento >= 3 || motivo === "DIRECCION_INCORRECTA" || motivo === "RECHAZADO") break;
    t = fin + (comp ? entre(rng, 5, 10) : entre(rng, 10, 60)) * MIN;
    mk("CLASIFICADO", t); // vuelve al circuito con nuevo intento
  }

  // --- Perturbaciones (para probar idempotencia, secuencia y DLQ) ---
  const out: Programado[] = [];
  const publicarEn = evs.map((e) => Date.parse(e.ocurridoEn));
  const sinPerturbar = g.limpio ?? false;
  if (!sinPerturbar) {
    for (let i = 0; i < evs.length - 1; i++) {
      const esResultado = evs[i].tipo === "ENTREGADO" || evs[i].tipo === "ENTREGA_FALLIDA";
      if (rng() < prob.fueraDeOrden && !esResultado && publicarEn[i + 1] - publicarEn[i] <= 3 * HORA) {
        [publicarEn[i], publicarEn[i + 1]] = [publicarEn[i + 1], publicarEn[i]]; // el evento i+1 sale antes que el i
        i++;
      }
    }
  }
  out.push({ at: t0, topic: TOPICS.PROMESAS, key: shipmentId, value: JSON.stringify({ eventId: idDeterministico(`${semilla}|${shipmentId}|promesa`), shipmentId, ocurridoEn: new Date(t0).toISOString(), version: 1, hubId: zona.hub, zonaId: zona.id, venceEn: new Date(venceEn).toISOString() }) });
  evs.forEach((e, i) => {
    const valor = JSON.stringify(e);
    out.push({ at: publicarEn[i], topic: TOPICS.ESTADOS, key: shipmentId, value: valor });
    if (sinPerturbar) return;
    if (rng() < prob.duplicado) out.push({ at: publicarEn[i] + Math.round(entre(rng, 1, 5) * MIN), topic: TOPICS.ESTADOS, key: shipmentId, value: valor });
    if (rng() < prob.malformado) out.push({ at: publicarEn[i], topic: TOPICS.ESTADOS, key: shipmentId, value: corromper(rng, e) });
  });
  return out;
}

/** Evento malformado: JSON roto o campos que violan el esquema. Debe terminar en la DLQ. */
export function corromper(rng: Rng, e: Estado): string {
  const v = Math.floor(rng() * 5);
  const copia: Record<string, unknown> = { ...e, eventId: idDeterministico(`malformado|${e.eventId}`) };
  if (v === 0) return JSON.stringify(copia).slice(0, 60); // JSON truncado
  if (v === 1) delete copia.tipo;
  else if (v === 2) copia.tipo = "VOLANDO";
  else if (v === 3) copia.secuencia = "dos";
  else delete copia.shipmentId;
  return JSON.stringify(copia);
}

class Cola {
  private a: Array<{ p: Programado; n: number }> = [];
  private n = 0;
  get length() { return this.a.length; }
  private menor(i: number, j: number) { return this.a[i].p.at < this.a[j].p.at || (this.a[i].p.at === this.a[j].p.at && this.a[i].n < this.a[j].n); }
  push(p: Programado) {
    this.a.push({ p, n: this.n++ });
    let i = this.a.length - 1;
    while (i > 0) { const q = (i - 1) >> 1; if (!this.menor(i, q)) break; [this.a[i], this.a[q]] = [this.a[q], this.a[i]]; i = q; }
  }
  peek(): Programado | undefined { return this.a[0]?.p; }
  pop(): Programado | undefined {
    if (!this.a.length) return undefined;
    const top = this.a[0].p;
    const last = this.a.pop()!;
    if (this.a.length) {
      this.a[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1, r = l + 1; let m = i;
        if (l < this.a.length && this.menor(l, m)) m = l;
        if (r < this.a.length && this.menor(r, m)) m = r;
        if (m === i) break;
        [this.a[i], this.a[m]] = [this.a[m], this.a[i]]; i = m;
      }
    }
    return top;
  }
}

export interface OpcionesSim {
  semilla: number;
  factor: number;
  enviosPorMinuto: number;
  inicioMs: number;
  prob: Probabilidades;
  /** Reloj real en ms (inyectable para pruebas). */
  ahoraReal?: () => number;
}
export type Escenario = "feliz" | "ausente" | "demorado" | "tercer-fallo" | "rafaga";
export const ESCENARIOS: Escenario[] = ["feliz", "ausente", "demorado", "tercer-fallo", "rafaga"];

/**
 * Simulador: planifica los eventos de cada envío en TIEMPO SIMULADO (con semilla fija, deterministas) y los
 * publica cuando el reloj simulado los alcanza. 1 hora simulada = 3600/factor segundos reales.
 */
export class Simulador {
  private readonly rng: Rng;
  private readonly rngEsc: Rng;
  private readonly cola = new Cola();
  private readonly real: () => number;
  private readonly inicioReal: number;
  private numero = 0;
  private proxima: number;
  private conteoEsc: Record<string, number> = {};
  /** Distingue los envíos de escenarios de distintas ejecuciones del simulador (evita que se descarten como duplicados). */
  private readonly corrida = Date.now().toString(36).slice(-4).toUpperCase();
  publicados = 0;
  continuo = true;
  ultimoEstado: Programado | null = null;

  constructor(private readonly o: OpcionesSim, private readonly publicar: (m: Programado) => Promise<void>) {
    this.rng = mulberry32(o.semilla);
    this.rngEsc = mulberry32(o.semilla + 1000);
    this.real = o.ahoraReal ?? Date.now;
    this.inicioReal = this.real();
    this.proxima = o.inicioMs;
  }

  simAhora(): number { return this.o.inicioMs + (this.real() - this.inicioReal) * this.o.factor; }
  get creados() { return this.numero; }
  get enCola() { return this.cola.length; }

  /** Crea los envíos que tocan y publica todo lo que ya "ocurrió" según el reloj simulado. */
  async bombear(simHasta: number = this.simAhora()): Promise<void> {
    if (this.continuo) {
      const intervalo = (60_000 * this.o.factor) / this.o.enviosPorMinuto;
      while (this.proxima <= simHasta) {
        this.numero++;
        this.agendar(generarEnvio(this.rng, this.o.semilla, `ENV-${pad(this.numero, 6)}`, Math.round(this.proxima), this.o.prob));
        this.proxima += intervalo * (0.7 + 0.6 * this.rng());
      }
    }
    for (let m = this.cola.peek(); m && m.at <= simHasta; m = this.cola.peek()) {
      this.cola.pop();
      await this.publicar(m);
      this.publicados++;
      if (m.topic === TOPICS.ESTADOS) this.ultimoEstado = m;
    }
  }
  private agendar(ps: Programado[]) { ps.forEach((p) => this.cola.push(p)); }

  /** Escenarios que se disparan a pedido durante la demo. Devuelve los ids creados. */
  escenario(nombre: Escenario, ahora: number = this.simAhora()): string[] {
    const n = (this.conteoEsc[nombre] = (this.conteoEsc[nombre] ?? 0) + 1);
    const nid = (p: string, i = n) => `DEMO-${p}-${this.corrida}-${pad(i, 2)}`;
    const t0 = Math.round(ahora);
    const base = { limpio: true, comprimido: true };
    const prob0: Probabilidades = { demoraHub: 0, duplicado: 0, fueraDeOrden: 0, malformado: 0 };
    const crear = (id: string, t: number, g: Guion) => this.agendar(generarEnvio(this.rngEsc, this.o.semilla, id, t, g.limpio ? prob0 : this.o.prob, g));
    switch (nombre) {
      case "feliz": crear(nid("FELIZ"), t0, { ...base, zonaId: "Z1" }); return [nid("FELIZ")];
      case "ausente": crear(nid("AUSENTE"), t0, { ...base, zonaId: "Z5", fallas: ["AUSENTE"] }); return [nid("AUSENTE")];
      case "demorado":
        // Promesa en 6 h simuladas y el paquete queda 8 h detenido en el hub: alerta ALTA a las 2 h, CRÍTICA a las 6 h.
        crear(nid("DEMORADO"), t0, { ...base, zonaId: "Z3", promesaMs: 6 * HORA, demoraHubMs: 8 * HORA });
        return [nid("DEMORADO")];
      case "tercer-fallo": crear(nid("TERCER-FALLO"), t0, { ...base, zonaId: "Z6", fallas: ["AUSENTE", "AUSENTE", "AUSENTE"], reintentoMs: 40 * MIN }); return [nid("TERCER-FALLO")];
      case "rafaga": {
        const ids: string[] = [];
        for (let i = 1; i <= 500; i++) {
          const id = `RAF-${this.corrida}-${pad(n, 2)}-${pad(i, 3)}`;
          ids.push(id);
          crear(id, t0 + Math.round((i * 10 * MIN) / 500), { limpio: true }); // 500 envíos en ~5 s reales
        }
        return ids;
      }
    }
  }

  /** Inyecta a mano un evento inválido o repetido (pasos "Evento inválido" y de idempotencia de la demo). */
  inyectar(tipo: "malformado" | "duplicado"): { topic: string; descripcion: string } {
    const ahora = this.simAhora();
    if (tipo === "duplicado" && this.ultimoEstado) {
      this.cola.push({ ...this.ultimoEstado, at: ahora });
      return { topic: TOPICS.ESTADOS, descripcion: "se reenvía el último evento publicado (mismo eventId)" };
    }
    const k = (this.conteoEsc.malformado = (this.conteoEsc.malformado ?? 0) + 1);
    const malo = { eventId: `demo-malformado-${k}`, shipmentId: "DEMO-MALFORMADO", tipo: "VOLANDO", ocurridoEn: new Date(ahora).toISOString(), secuencia: "uno", version: 1 };
    this.cola.push({ at: ahora, topic: TOPICS.ESTADOS, key: "DEMO-MALFORMADO", value: JSON.stringify(malo) });
    return { topic: TOPICS.ESTADOS, descripcion: "evento con tipo inválido y secuencia no numérica" };
  }
}

export { MOTIVOS };
