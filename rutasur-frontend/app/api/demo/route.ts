import { NextRequest, NextResponse } from "next/server";
export const dynamic = "force-dynamic";
const base = process.env.SIMULADOR_URL ?? "http://localhost:4000";
export async function GET() {
  try { const r = await fetch(`${base}/status`, { cache: "no-store" }); return NextResponse.json(await r.json(), { status: r.status }); }
  catch (error) { return NextResponse.json({ error: `No se pudo conectar con el simulador: ${String(error)}` }, { status: 503 }); }
}
export async function POST(req: NextRequest) {
  try {
    const body = await req.json() as { accion?: string; valor?: string };
    const allowed = ["feliz", "ausente", "demorado", "tercer-fallo", "rafaga"];
    let path = "";
    if (body.accion === "escenario" && body.valor && allowed.includes(body.valor)) path = `/scenarios/${body.valor}`;
    else if (body.accion === "inject" && ["malformado", "duplicado"].includes(body.valor ?? "")) path = `/inject/${body.valor}`;
    else if (body.accion === "continuo" && ["on", "off"].includes(body.valor ?? "")) path = `/continuo/${body.valor}`;
    else return NextResponse.json({ error: "Acción no permitida" }, { status: 400 });
    const r = await fetch(`${base}${path}`, { method: "POST", cache: "no-store" });
    return NextResponse.json(await r.json(), { status: r.status });
  } catch (error) { return NextResponse.json({ error: String(error) }, { status: 503 }); }
}
