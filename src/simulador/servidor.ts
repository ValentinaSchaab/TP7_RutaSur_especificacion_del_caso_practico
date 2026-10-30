import { createServer, Server } from "http";
import { ESCENARIOS, Escenario, Simulador } from "./engine";

/** API de control del simulador (la usa el panel de demo de Next.js; no toca Kafka ni MongoDB). */
export function iniciarServidor(sim: Simulador, puerto: number, log: (m: string) => void): Server {
  const srv = createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "content-type");
    const responder = (code: number, cuerpo: unknown) => { res.writeHead(code, { "content-type": "application/json" }); res.end(JSON.stringify(cuerpo)); };
    if (req.method === "OPTIONS") return responder(204, {});
    const url = (req.url ?? "/").split("?")[0];
    const [, ruta, param] = url.split("/");
    if (req.method === "GET" && ruta === "status")
      return responder(200, { simAhora: new Date(sim.simAhora()).toISOString(), enCola: sim.enCola, publicados: sim.publicados, creados: sim.creados, continuo: sim.continuo, escenarios: ESCENARIOS });
    if (req.method === "POST" && ruta === "scenarios" && ESCENARIOS.includes(param as Escenario)) {
      const ids = sim.escenario(param as Escenario);
      log(`Escenario "${param}" disparado (${ids.length} envío/s)`);
      return responder(202, { escenario: param, envios: ids.length, primero: ids[0] });
    }
    if (req.method === "POST" && ruta === "inject" && (param === "malformado" || param === "duplicado")) {
      const r = sim.inyectar(param);
      log(`Inyección "${param}": ${r.descripcion}`);
      return responder(202, r);
    }
    if (req.method === "POST" && ruta === "continuo" && (param === "on" || param === "off")) {
      sim.continuo = param === "on";
      return responder(200, { continuo: sim.continuo });
    }
    responder(404, { error: "no encontrado" });
  });
  srv.listen(puerto);
  return srv;
}
