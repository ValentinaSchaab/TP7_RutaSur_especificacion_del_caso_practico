export interface Logger {
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
}
export function crearLog(servicio: string): Logger {
  const fmt = (nivel: string, msg: string) => `${new Date().toISOString()} ${nivel} [${servicio}] ${msg}`;
  return {
    info: (m) => console.log(fmt("INFO ", m)),
    warn: (m) => console.warn(fmt("WARN ", m)),
    error: (m) => console.error(fmt("ERROR", m)),
  };
}
export const logSilencioso: Logger = { info() {}, warn() {}, error() {} };
