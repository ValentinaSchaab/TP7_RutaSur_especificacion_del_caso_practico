// Test de punta a punta: servicios reales + MongoDB real (replica set) + bus en memoria con semántica de Kafka.
process.env.RETRY_BACKOFF_MS = "10,20,40";
const assert = require("assert");
const path = require("path");
const { execSync } = require("child_process");
const d = (p) => require(path.join(__dirname, "..", "dist", p));
const { InMemoryBus } = d("lib/bus");
const { conectar, prepararBase } = d("lib/mongo");
const { logSilencioso: _silencio, crearLog } = d("lib/log"); const logSilencioso = process.env.DEBUG ? crearLog("e2e") : _silencio;
const { RelojSim } = d("lib/tiempo");
const { TOPICS } = d("lib/topics");
const { Simulador, generarEnvio, mulberry32 } = d("simulador/engine");
const svc = (n) => d(`services/${n}`);

const H = 3600e3, M = 60e3;
const BASE = Date.parse("2026-11-02T10:00:00.000Z"); // lunes 10:00 (UTC, tiempo simulado)
const iso = (ms) => new Date(ms).toISOString();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let n = 0;
const ok = (m) => { n++; console.log("  ✔", m); };

(async () => {
  const URI = "mongodb://127.0.0.1:27017/rutasur_test?directConnection=true";
  const mongo = await conectar(URI);
  await mongo.db.dropDatabase();
  await prepararBase(mongo.db);
  const bus = new InMemoryBus();
  const deps = { bus, mongo, log: logSilencioso };
  const col = (c) => mongo.db.collection(c);
  const quieto = (ms = 150) => bus.cuandoEsteQuieto(ms);
  const topic = (t) => bus.mensajes(t).map((m) => JSON.parse(m.value));
  const raw = (t) => bus.mensajes(t);

  const ev = (id, seq, tipo, at, extra = {}) => ({ eventId: `evt-${id}-${seq}`, shipmentId: id, tipo, ocurridoEn: iso(at), secuencia: seq, hubId: "H1", zonaId: "Z1", repartidorId: "R001", version: 1, ...extra });
  const pub = (t, e) => bus.publish({ topic: t, key: e.shipmentId, value: JSON.stringify(e) });
  const promesa = (id, at, venceEn) => pub(TOPICS.PROMESAS, { eventId: `promesa-${id}`, shipmentId: id, ocurridoEn: iso(at), version: 1, hubId: "H1", zonaId: "Z1", venceEn: iso(venceEn) });
  async function ciclo(id, t, hasta = "ENTREGADO", extra = {}) {
    const orden = ["CREADO", "INGRESADO_HUB", "CLASIFICADO", "EN_REPARTO", "ENTREGADO"];
    for (let i = 0; i < orden.length; i++) {
      await pub(TOPICS.ESTADOS, ev(id, i + 1, orden[i], t + i * 10 * M, orden[i] === "ENTREGADO" ? { intento: 1, ...extra } : extra));
      if (orden[i] === hasta) break;
    }
  }

  const reloj = new RelojSim(0);
  const proyector = await svc("proyector").iniciar(deps);
  let notificaciones = await svc("notificaciones").iniciar(deps);
  const reprogramacion = await svc("reprogramacion").iniciar(deps);
  const analitica = await svc("analitica").iniciar(deps);
  let monitor = await svc("monitor-sla").iniciar(deps, { reloj, tickMs: 3600e3 });
  console.log("Servicios en marcha. Verificaciones:\n\n[Ciclo de vida]");

  // --- CU1 ---
  await ciclo("T1", BASE); await quieto();
  let e1 = await col("envios").findOne({ _id: "T1" });
  assert.equal(e1.estado, "ENTREGADO"); assert.equal(e1.historial.filter((h) => h.origen === "estados").length, 5); assert.equal(e1.situacion, "ENTREGADO");
  ok("Un envío recorre CREADO→ENTREGADO y el documento refleja el estado final y su historial completo");

  await pub(TOPICS.ESTADOS, ev("T1", 2, "INGRESADO_HUB", BASE + 10 * M)); // mismo eventId que el original
  await quieto();
  e1 = await col("envios").findOne({ _id: "T1" });
  assert.equal(e1.historial.length, 5);
  ok("Un evento duplicado (mismo eventId) no agrega una entrada repetida al historial");

  await pub(TOPICS.ESTADOS, ev("T2", 1, "CREADO", BASE));
  await pub(TOPICS.ESTADOS, ev("T2", 3, "CLASIFICADO", BASE + 20 * M));
  await pub(TOPICS.ESTADOS, ev("T2", 2, "INGRESADO_HUB", BASE + 10 * M)); // llega tarde
  await quieto();
  const e2 = await col("envios").findOne({ _id: "T2" });
  assert.equal(e2.estado, "CLASIFICADO"); assert.equal(e2.ultimaSecuencia, 3); assert.equal(e2.historial.length, 2);
  ok("Un evento fuera de orden (secuencia menor) no hace retroceder el estado del envío");

  await bus.publish({ topic: TOPICS.ESTADOS, key: "T3", value: '{"eventId":"roto-1","shipmentId":"T3","tipo":"CRE' });
  await bus.publish({ topic: TOPICS.ESTADOS, key: "T3", value: JSON.stringify({ ...ev("T3", 1, "VOLANDO", BASE) }) });
  await ciclo("T4", BASE); await quieto();
  const dlq = raw(TOPICS.DLQ);
  assert.equal(dlq.length, 2); assert.ok(dlq.every((m) => m.headers["x-error"] && m.headers["x-topic-origen"] === TOPICS.ESTADOS));
  assert.equal((await col("envios").findOne({ _id: "T4" })).estado, "ENTREGADO");
  ok(`Un evento malformado termina en la DLQ con su motivo ("${dlq[1].headers["x-error"].slice(0, 45)}…") y el consumo continúa`);

  // --- CU2 ---
  console.log("\n[Entrega fallida]");
  const fallo = async (id, t, motivo, intento = 1, zona = "Z1") => {
    await pub(TOPICS.ESTADOS, ev(id, 10 + intento, "ENTREGA_FALLIDA", t, { intento, motivo, zonaId: zona }));
  };
  await ciclo("F1", BASE, "EN_REPARTO"); await fallo("F1", BASE + 60 * M, "AUSENTE"); await quieto();
  assert.equal(topic(TOPICS.NOTIFICACIONES).filter((x) => x.shipmentId === "F1").length, 1);
  assert.equal(await col("notificaciones").countDocuments({ shipmentId: "F1" }), 1);
  const rp = topic(TOPICS.REPROGRAMACIONES).filter((x) => x.shipmentId === "F1");
  assert.equal(rp.length, 1);
  ok("Un ENTREGA_FALLIDA genera exactamente una notificación y una reprogramación");
  assert.equal(rp[0].tipo, "REPROGRAMADO"); assert.deepEqual(rp[0].nuevoIntento, { fecha: "2026-11-03", franja: "TARDE" });
  ok("AUSENTE (lunes por la mañana): nuevo intento el martes, en la franja opuesta (TARDE)");
  const ef1 = await col("envios").findOne({ _id: "F1" });
  assert.equal(ef1.situacion, "REPROGRAMADO");
  assert.ok(ef1.historial.some((h) => h.origen === "notificaciones") && ef1.historial.some((h) => h.origen === "reprogramaciones"));
  ok("El proyector refleja el fallo, el aviso y el nuevo intento en el documento del envío");
  const met = await col("metricas_diarias").findOne({ fecha: "2026-11-02", zonaId: "Z1", franja: "MANANA" });
  assert.equal(met.motivos.AUSENTE, 1);
  ok("Analítica suma el fallo a los agregados por zona, franja y motivo");

  await fallo("F2", BASE + 4 * 24 * H + 5 * H, "AUSENTE"); // viernes 6-nov a las 15:00 -> TARDE
  await fallo("F3", BASE, "DIRECCION_INCORRECTA"); await fallo("F4", BASE, "RECHAZADO");
  await fallo("F5", BASE + 5 * H, "ZONA_INACCESIBLE"); await fallo("F6", BASE, "AUSENTE", 3); await quieto();
  const regla = (id) => topic(TOPICS.REPROGRAMACIONES).find((x) => x.shipmentId === id);
  assert.equal(regla("F2").nuevoIntento.fecha, "2026-11-09"); assert.equal(regla("F2").nuevoIntento.franja, "MANANA");
  ok("AUSENTE un viernes por la tarde: nuevo intento el lunes hábil, en la franja MANANA");
  assert.equal(regla("F3").tipo, "RETENIDO"); assert.equal((await col("envios").findOne({ _id: "F3" })).situacion, "RETENIDO");
  ok("DIRECCION_INCORRECTA: el envío queda RETENIDO y no se reprograma solo");
  assert.equal(regla("F4").tipo, "DEVOLUCION"); assert.equal(regla("F4").causa, "RECHAZADO");
  ok("RECHAZADO: se marca para devolución al comercio");
  assert.equal(regla("F5").tipo, "REPROGRAMADO"); assert.equal(regla("F5").nuevoIntento.franja, "TARDE");
  ok("ZONA_INACCESIBLE (no definida en el documento): se reprograma al día hábil siguiente en la misma franja");
  assert.equal(regla("F6").tipo, "DEVOLUCION"); assert.equal(regla("F6").causa, "TERCER_FALLO");
  assert.equal((await col("envios").findOne({ _id: "F6" })).situacion, "DEVOLUCION");
  ok("El tercer fallo (cualquier motivo) marca el envío para devolución");

  // Aislamiento / recuperación: se detiene notificaciones
  await notificaciones.detener();
  await fallo("F7", BASE + 2 * H, "AUSENTE"); await ciclo("F8", BASE, "ENTREGADO"); await quieto();
  assert.equal(topic(TOPICS.NOTIFICACIONES).filter((x) => x.shipmentId === "F7").length, 0);
  assert.equal((await col("envios").findOne({ _id: "F8" })).estado, "ENTREGADO");
  assert.equal(topic(TOPICS.REPROGRAMACIONES).filter((x) => x.shipmentId === "F7").length, 1);
  ok("Con notificaciones detenido, el tracking y la reprogramación siguen funcionando (aislamiento)");
  notificaciones = await svc("notificaciones").iniciar(deps); await quieto();
  assert.equal(topic(TOPICS.NOTIFICACIONES).filter((x) => x.shipmentId === "F7").length, 1);
  assert.equal(await col("notificaciones").countDocuments({ shipmentId: "F7" }), 1);
  ok("Al reiniciarlo procesa lo pendiente desde su último offset: un solo aviso, sin duplicados");

  // --- CU3 ---
  console.log("\n[Riesgo de SLA]");
  const SLAT = BASE + 7 * 24 * H - 4 * H; // lunes 9-nov 06:00: posterior a todo lo anterior (el reloj simulado solo avanza)
  const alertasDe = (id) => topic(TOPICS.ALERTAS).filter((x) => x.shipmentId === id);
  // Regla 1: sin ingreso (<10 h y todavía CREADO) -> MEDIA
  await promesa("S1", SLAT, SLAT + 8 * H); await pub(TOPICS.ESTADOS, ev("S1", 1, "CREADO", SLAT)); await quieto();
  assert.deepEqual(alertasDe("S1").map((a) => [a.regla, a.prioridad, a.tipo]), [["SIN_INGRESO", "MEDIA", "ALERTA_ABIERTA"]]);
  // Regla 2: sin salida (<4 h y no está en reparto) -> ALTA
  await promesa("S2", SLAT, SLAT + 3 * H); await pub(TOPICS.ESTADOS, ev("S2", 1, "CREADO", SLAT)); await pub(TOPICS.ESTADOS, ev("S2", 2, "INGRESADO_HUB", SLAT + 5 * M)); await quieto();
  assert.deepEqual(alertasDe("S2").filter((a) => a.regla === "SIN_SALIDA").map((a) => a.prioridad), ["ALTA"]);
  // Regla 3: fallo en el día límite -> ALTA
  await promesa("S3", SLAT, SLAT + 9 * H); await pub(TOPICS.ESTADOS, ev("S3", 1, "ENTREGA_FALLIDA", SLAT + 10 * M, { intento: 1, motivo: "AUSENTE" })); await quieto();
  assert.deepEqual(alertasDe("S3").map((a) => [a.regla, a.prioridad]), [["FALLO_DIA_LIMITE", "ALTA"]]);
  // Regla 4: vencido -> CRITICA (nace del PASO DEL TIEMPO, sin ningún evento del envío)
  await promesa("S4", SLAT, SLAT + 1 * H); await pub(TOPICS.ESTADOS, ev("S4", 1, "CLASIFICADO", SLAT)); await quieto();
  assert.deepEqual(alertasDe("S4").map((a) => a.regla), ["SIN_SALIDA"]); // vence en 1 h y no está en reparto
  await promesa("S5", SLAT, SLAT + 11 * H); await pub(TOPICS.ESTADOS, ev("S5", 1, "CREADO", SLAT)); await quieto();
  assert.equal(alertasDe("S5").length, 0);
  await pub(TOPICS.ESTADOS, ev("OTRO", 1, "CREADO", SLAT + 2 * H)); await quieto(); // el reloj simulado avanza 2 h (viaja en los eventos)
  await monitor.tick(); await quieto();
  assert.deepEqual(alertasDe("S4").map((a) => [a.regla, a.prioridad]), [["SIN_SALIDA", "ALTA"], ["VENCIDO", "CRITICA"]]);
  assert.deepEqual(alertasDe("S5").map((a) => [a.regla, a.prioridad]), [["SIN_INGRESO", "MEDIA"]]);
  ok("Las cuatro reglas generan su alerta con la prioridad definida (MEDIA, ALTA, ALTA, CRITICA)");
  ok("La alerta nace de evaluar el tiempo, no de un evento: S4 (vencido) y S5 (sin ingreso) alertaron sin recibir ningún evento propio");

  const antes = topic(TOPICS.ALERTAS).length;
  await monitor.tick(); await monitor.tick(); await pub(TOPICS.ESTADOS, ev("S2", 3, "CLASIFICADO", SLAT + 20 * M)); await quieto();
  assert.equal(topic(TOPICS.ALERTAS).length, antes);
  ok("Una misma regla no alerta dos veces sobre el mismo envío (ticks y eventos repetidos)");

  // Reinicio del monitor: estado recuperado desde MongoDB
  await monitor.detener();
  const reloj2 = new RelojSim(0);
  monitor = await svc("monitor-sla").iniciar(deps, { reloj: reloj2, tickMs: 3600e3 });
  await monitor.tick(); await quieto();
  assert.equal(topic(TOPICS.ALERTAS).length, antes);
  ok("Tras reiniciar el monitor no se pierden ni se repiten alertas (estado y reloj recuperados de MongoDB)");

  await pub(TOPICS.ESTADOS, ev("S2", 4, "EN_REPARTO", SLAT + 3 * H)); await pub(TOPICS.ESTADOS, ev("S2", 5, "ENTREGADO", SLAT + 150 * M, { intento: 1 }));
  await pub(TOPICS.ESTADOS, ev("S4", 2, "ENTREGADO", SLAT + 3 * H, { intento: 1 })); await quieto();
  for (const [id, regla] of [["S2", "SIN_SALIDA"], ["S4", "VENCIDO"]]) {
    assert.deepEqual(alertasDe(id).filter((a) => a.regla === regla).map((a) => a.tipo), ["ALERTA_ABIERTA", "ALERTA_CERRADA"]);
    assert.equal((await col("alertas").findOne({ _id: `${id}:${regla}` })).estado, "CERRADA");
  }
  ok("La alerta se cierra cuando el envío se entrega (ALERTA_CERRADA, incluso la CRITICA ya vencida)");
  assert.ok((await col("notificaciones").countDocuments({ shipmentId: "S3", tipo: "AVISO_ALERTA" })) === 1);
  ok("Las alertas ALTA/CRITICA generan un aviso al destinatario (notificaciones.enviadas)");

  // --- CU4 ---
  console.log("\n[Simulador, analítica y reprocesamiento]");
  const g = (s) => { const r = mulberry32(s); return Array.from({ length: 40 }, (_, i) => generarEnvio(r, s, `ENV-${i}`, BASE + i * 6 * M, { demoraHub: 0.1, duplicado: 0.02, fueraDeOrden: 0.02, malformado: 0.01 })); };
  assert.deepEqual(g(42), g(42)); assert.notDeepEqual(g(42), g(43));
  ok("Semilla fija: la misma semilla reproduce exactamente los mismos eventos (y otra semilla, otros)");

  await mongo.db.dropDatabase(); await prepararBase(mongo.db);
  await Promise.all([proyector.detener(), notificaciones.detener(), reprogramacion.detener(), analitica.detener(), monitor.detener()]);
  const bus2 = new InMemoryBus(); const deps2 = { bus: bus2, mongo, log: logSilencioso };
  const reloj3 = new RelojSim(0);
  const s2 = { proyector: await svc("proyector").iniciar(deps2), notif: await svc("notificaciones").iniciar(deps2), rep: await svc("reprogramacion").iniciar(deps2), an: await svc("analitica").iniciar(deps2), mon: await svc("monitor-sla").iniciar(deps2, { reloj: reloj3, tickMs: 3600e3 }) };
  let t = BASE; const pubSim = (m) => bus2.publish({ topic: m.topic, key: m.key, value: m.value });
  const sim = new Simulador({ semilla: 42, factor: 120, enviosPorMinuto: 20, inicioMs: BASE, prob: { demoraHub: 0.1, duplicado: 0.02, fueraDeOrden: 0.02, malformado: 0.01 }, ahoraReal: () => t }, pubSim);
  const paso = 30 * M / 120; // avanza 30 min simulados por paso
  for (let i = 0; i < 24; i++) { t += paso; await sim.bombear(); } // 12 h simuladas de creación
  sim.continuo = false;
  for (let i = 0; i < 160; i++) { t += paso; await sim.bombear(); } // el resto de los ciclos de vida (reintentos incluidos)
  await bus2.cuandoEsteQuieto(300);
  const nEnvios = await col("envios").countDocuments();
  const malformados = bus2.mensajes(TOPICS.ESTADOS).filter((m) => { try { return !require(path.join(__dirname, "..", "dist", "lib/schemas")).estadoSchema.safeParse(JSON.parse(m.value)).success; } catch { return true; } }).length;
  assert.ok(nEnvios >= 100);
  assert.equal(bus2.mensajes(TOPICS.DLQ).length, malformados); assert.ok(malformados > 0);
  const tasa = await col("metricas_diarias").aggregate([{ $group: { _id: null, t: { $sum: "$primerIntento.total" }, e: { $sum: "$primerIntento.exitosos" } } }]).toArray();
  const pct = (100 * tasa[0].e) / tasa[0].t;
  assert.ok(pct > 70 && pct < 95, `tasa de primer intento ${pct}`);
  ok(`Corrida con semilla fija: ${nEnvios} envíos; ${malformados} eventos malformados → exactamente ${malformados} en la DLQ; entrega en primer intento ${pct.toFixed(1)} %`);

  const salida = execSync("node dist/tools/recuento.js", { cwd: path.join(__dirname, ".."), env: { ...process.env, MONGO_URI: URI } }).toString();
  assert.match(salida, /OK: los indicadores coinciden/);
  ok("Los tres indicadores coinciden con el recuento independiente (" + salida.split("\n")[0].replace(/^\d+ envíos revisados, /, "").slice(0, 70) + "…)");

  // Cuarto indicador: un grupo NUEVO lee el histórico desde el inicio
  assert.equal(await col("metricas_diarias").countDocuments({ "tiempoHub.cantidad": { $gt: 0 } }), 0);
  const antesProy = await col("envios").countDocuments(); const antesAn = JSON.stringify(await col("metricas_diarias").find({}, { projection: { tiempoHub: 0 } }).sort({ fecha: 1, zonaId: 1, franja: 1 }).toArray());
  const tiempos = await svc("analitica-tiempos").iniciar(deps2); await bus2.cuandoEsteQuieto(300);
  const porHub = await col("metricas_diarias").aggregate([{ $group: { _id: "$hubId", c: { $sum: "$tiempoHub.cantidad" }, s: { $sum: "$tiempoHub.sumaMs" } } }]).toArray();
  const man = {}; const vistos = {};
  for (const m of bus2.mensajes(TOPICS.ESTADOS)) { let e; try { e = JSON.parse(m.value); } catch { continue; } if (!require(path.join(__dirname, "..", "dist", "lib/schemas")).estadoSchema.safeParse(e).success) continue;
    const v = (vistos[e.shipmentId] ??= {}); if (e.tipo === "INGRESADO_HUB" && v.i === undefined) v.i = Date.parse(e.ocurridoEn); if (e.tipo === "EN_REPARTO" && v.r === undefined) { v.r = Date.parse(e.ocurridoEn); v.h = e.hubId; } }
  for (const v of Object.values(vistos)) if (v.i !== undefined && v.r !== undefined) { (man[v.h] ??= { c: 0, s: 0 }).c++; man[v.h].s += v.r - v.i; }
  for (const h of porHub) { assert.equal(h.c, man[h._id].c); assert.equal(h.s, man[h._id].s); }
  assert.ok(porHub.length === 3);
  assert.equal(await col("envios").countDocuments(), antesProy);
  assert.equal(JSON.stringify(await col("metricas_diarias").find({}, { projection: { tiempoHub: 0 } }).sort({ fecha: 1, zonaId: 1, franja: 1 }).toArray()), antesAn);
  ok("El cuarto indicador (tiempo medio INGRESADO_HUB→EN_REPARTO por hub) se completa con el histórico sin tocar los demás servicios: " + porHub.map((h) => `${h._id}=${(h.s / h.c / H).toFixed(1)} h`).sort().join(", "));

  // Reconstrucción: borrar `envios` y reprocesar deja el mismo resultado
  const canon = (x) => Array.isArray(x) ? x.map(canon) : x && typeof x === "object" && !(x instanceof Date) ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, canon(x[k])])) : x;
  const snap = async () => JSON.stringify(canon(await col("envios").find().sort({ _id: 1 }).toArray()));
  const alSnap = async () => JSON.stringify(canon(await col("alertas").find().sort({ _id: 1 }).toArray()));
  const antesEnv = await snap(); const antesAl = await alSnap(); const antesNo = await col("notificaciones").countDocuments();
  await s2.proyector.detener();
  await Promise.all(["envios", "alertas", "notificaciones"].map((c) => col(c).deleteMany({}))); await col("eventos_procesados").deleteMany({ consumidor: "proyector" });
  assert.equal(await col("envios").countDocuments(), 0);
  bus2.retrocederGrupo("proyector");
  const proy2 = await svc("proyector").iniciar(deps2); await bus2.cuandoEsteQuieto(300);
  if ((await snap()) !== antesEnv) {
    const A = JSON.parse(antesEnv), B = JSON.parse(await snap()); let k = 0;
    for (const a of A) { const b = B.find((x) => x._id === a._id); for (const f of new Set([...Object.keys(a), ...Object.keys(b ?? {})])) if (JSON.stringify(a[f]) !== JSON.stringify(b?.[f]) && k++ < 3) console.log("DIF", a._id, f, "\n  antes:", JSON.stringify(a[f])?.slice(0, 400), "\n  ahora:", JSON.stringify(b?.[f])?.slice(0, 400)); }
    console.log("docs antes/ahora:", A.length, B.length);
  }
  assert.equal(await snap(), antesEnv); assert.equal(await alSnap(), antesAl); assert.equal(await col("notificaciones").countDocuments(), antesNo);
  ok(`Borrar envios (${antesProy} documentos) y reprocesar el log deja la colección idéntica (también alertas y notificaciones)`);

  await Promise.all([proy2.detener(), s2.notif.detener(), s2.rep.detener(), s2.an.detener(), s2.mon.detener(), tiempos.detener()]);
  await mongo.client.close();
  console.log(`\n${n} verificaciones OK`);
  process.exit(0);
})().catch((e) => { console.error("\n✘ FALLÓ:", e.message, "\n", (e.stack || "").split("\n").slice(1, 5).join("\n")); process.exit(1); });
