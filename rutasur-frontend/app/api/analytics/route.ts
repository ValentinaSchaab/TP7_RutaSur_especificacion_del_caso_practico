import { NextRequest, NextResponse } from "next/server";
import { getDb, serializar } from "@/lib/mongo";
export const dynamic = "force-dynamic";
export async function GET(req: NextRequest) {
  try {
    const p = req.nextUrl.searchParams;
    const filter: Record<string, unknown> = {};
    for (const key of ["zonaId", "hubId", "fecha"]) { const value = p.get(key); if (value) filter[key] = value; }
    const docs = await (await getDb()).collection("metricas_diarias").find(filter).sort({ fecha: -1 }).limit(500).toArray();
    const sum = (fn: (d: any) => number) => docs.reduce((a, d) => a + (Number(fn(d)) || 0), 0);
    const total = sum(d => d.primerIntento?.total);
    const exitosos = sum(d => d.primerIntento?.exitosos);
    const entregados = sum(d => d.sla?.entregados);
    const aTiempo = sum(d => d.sla?.aTiempo);
    const tiempoCantidad = sum(d => d.tiempoHub?.cantidad);
    const tiempoMs = sum(d => d.tiempoHub?.sumaMs);
    const motivos: Record<string, number> = {};
    for (const d of docs) for (const [motivo, cantidad] of Object.entries(d.motivos ?? {})) motivos[motivo] = (motivos[motivo] ?? 0) + Number(cantidad || 0);
    return NextResponse.json(serializar({
      filas: docs, tasaPrimerIntento: total ? exitosos / total * 100 : 0,
      cumplimientoSla: entregados ? aTiempo / entregados * 100 : 0,
      primerIntento: { total, exitosos }, sla: { entregados, aTiempo }, motivos,
      tiempoMedioHubHoras: tiempoCantidad ? tiempoMs / tiempoCantidad / 3600000 : null,
      cantidadTiemposHub: tiempoCantidad
    }));
  } catch (error) { return NextResponse.json({ error: String(error) }, { status: 503 }); }
}
