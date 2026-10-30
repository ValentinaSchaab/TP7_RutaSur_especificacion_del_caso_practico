/**
 * Punto de entrada de cada servicio: `node dist/bin.js <servicio>`.
 * Cada servicio es un proceso Node.js separado (se puede detener uno sin afectar a los demás).
 */
import { KafkaBus } from "./lib/bus";
import { config } from "./lib/config";
import { crearLog } from "./lib/log";
import { conectar, prepararBase } from "./lib/mongo";
import { Servicio } from "./services/comun";
import { Simulador } from "./simulador/engine";
import { iniciarServidor } from "./simulador/servidor";

const SERVICIOS = ["simulador", "proyector", "notificaciones", "reprogramacion", "monitor-sla", "analitica", "analitica-tiempos"] as const;
type Nombre = (typeof SERVICIOS)[number];

async function main() {
  const nombre = process.argv[2] as Nombre;
  if (!SERVICIOS.includes(nombre)) {
    console.error(`Uso: node dist/bin.js <${SERVICIOS.join("|")}>`);
    process.exit(2);
  }
  const log = crearLog(nombre);
  const bus = new KafkaBus(config.brokers, `rutasur-${nombre}`);
  let detener: () => Promise<void> = async () => undefined;
  let cerrarMongo: () => Promise<void> = async () => undefined;

  if (nombre === "simulador") {
    const sim = new Simulador(
      { semilla: config.seed, factor: config.factor, enviosPorMinuto: config.enviosPorMinuto, inicioMs: Date.parse(config.simInicio), prob: config.prob },
      (m) => bus.publish({ topic: m.topic, key: m.key, value: m.value }),
    );
    sim.continuo = config.autoStart;
    let ocupado = false;
    const timer = setInterval(() => {
      if (ocupado) return;
      ocupado = true;
      sim.bombear().catch((e) => log.error(`publicar: ${(e as Error).message}`)).finally(() => { ocupado = false; });
    }, 100);
    const srv = iniciarServidor(sim, config.simPort, (m) => log.info(m));
    log.info(`Simulador en marcha: semilla=${config.seed}, ${config.enviosPorMinuto} envíos/min, 1 h simulada = ${(3600 / config.factor).toFixed(0)} s. API de control en :${config.simPort}`);
    detener = async () => { clearInterval(timer); srv.close(); };
  } else {
    const mongo = await conectar(config.mongoUri);
    await prepararBase(mongo.db);
    cerrarMongo = () => mongo.client.close();
    const deps = { bus, mongo, log };
    const modulo = (await import(`./services/${nombre}`)) as { iniciar(d: typeof deps): Promise<Servicio> };
    const svc = await modulo.iniciar(deps);
    log.info("Servicio iniciado");
    detener = svc.detener;
  }

  const apagar = async () => {
    log.info("Apagando...");
    await detener();
    await bus.disconnect();
    await cerrarMongo();
    process.exit(0);
  };
  process.on("SIGTERM", apagar);
  process.on("SIGINT", apagar);
}

main().catch((e) => {
  console.error(e);
  process.exit(1); // Docker (restart: unless-stopped) lo vuelve a levantar
});
