/**
 * Recuento independiente de los indicadores: recalcula desde cero los 3 indicadores a partir del
 * HISTORIAL de cada envío (colección `envios`, escrita por otro servicio) y los compara con los
 * agregados incrementales de `metricas_diarias` (escritos por analítica).
 *
 *   pnpm run recuento      (conviene correrlo con el lag de los grupos en 0)
 */
import { config } from "../lib/config";
import { MOTIVOS } from "../lib/schemas";
import { fechaDe, franjaDe } from "../lib/tiempo";
import { conectar } from "../lib/mongo";

type Acum = { total: number; exitosos: number; entregados: number; aTiempo: number; motivos: Record<string, number> };
const vacio = (): Acum => ({ total: 0, exitosos: 0, entregados: 0, aTiempo: 0, motivos: Object.fromEntries(MOTIVOS.map((m) => [m, 0])) });

async function main() {
  const { client, db } = await conectar(config.mongoUri);
  const manual = new Map<string, Acum>();
  let envios = 0;
  for await (const e of db.collection("envios").find()) {
    envios++;
    for (const h of e.historial ?? []) {
      if (h.origen !== "estados" || (h.tipo !== "ENTREGADO" && h.tipo !== "ENTREGA_FALLIDA")) continue;
      const k = `${fechaDe(h.ocurridoEn)}|${e.zonaId}|${franjaDe(h.ocurridoEn)}`;
      const a = manual.get(k) ?? vacio();
      if (h.intento === 1) { a.total++; if (h.tipo === "ENTREGADO") a.exitosos++; }
      if (h.tipo === "ENTREGA_FALLIDA") a.motivos[h.motivo]++;
      if (h.tipo === "ENTREGADO") { a.entregados++; if (e.promesa?.venceEn && Date.parse(h.ocurridoEn) <= new Date(e.promesa.venceEn).getTime()) a.aTiempo++; }
      manual.set(k, a);
    }
  }
  let diferencias = 0;
  const claves = new Set(manual.keys());
  const docs = await db.collection("metricas_diarias").find().toArray();
  for (const d of docs) claves.add(`${d.fecha}|${d.zonaId}|${d.franja}`);
  for (const k of [...claves].sort()) {
    const m = manual.get(k) ?? vacio();
    const d = docs.find((x) => `${x.fecha}|${x.zonaId}|${x.franja}` === k);
    const analitica = { total: d?.primerIntento?.total ?? 0, exitosos: d?.primerIntento?.exitosos ?? 0, entregados: d?.sla?.entregados ?? 0, aTiempo: d?.sla?.aTiempo ?? 0 };
    const mismo = analitica.total === m.total && analitica.exitosos === m.exitosos && analitica.entregados === m.entregados && analitica.aTiempo === m.aTiempo && MOTIVOS.every((mo) => (d?.motivos?.[mo] ?? 0) === m.motivos[mo]);
    if (!mismo) { diferencias++; console.log(`DIFERENCIA en ${k}: analítica=${JSON.stringify(analitica)} motivos=${JSON.stringify(d?.motivos)} | recuento=${JSON.stringify({ ...m, motivos: m.motivos })}`); }
  }
  const tasa = [...manual.values()].reduce((s, a) => ({ t: s.t + a.total, e: s.e + a.exitosos }), { t: 0, e: 0 });
  console.log(`${envios} envíos revisados, ${claves.size} combinaciones fecha/zona/franja. Tasa de entrega en primer intento: ${tasa.t ? ((100 * tasa.e) / tasa.t).toFixed(1) : "-"} % (${tasa.e}/${tasa.t}).`);
  console.log(diferencias === 0 ? "OK: los indicadores coinciden con el recuento manual." : `ATENCIÓN: ${diferencias} combinaciones no coinciden (¿hay lag sin procesar?).`);
  await client.close();
  process.exit(diferencias === 0 ? 0 : 1);
}
main().catch((e) => { console.error(e); process.exit(1); });
