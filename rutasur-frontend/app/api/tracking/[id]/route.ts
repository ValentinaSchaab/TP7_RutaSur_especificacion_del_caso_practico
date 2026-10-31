import { NextRequest, NextResponse } from "next/server";
import { getDb, serializar } from "@/lib/mongo";
export const dynamic = "force-dynamic";
export async function GET(_req: NextRequest, context: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await context.params;
    const doc = await (await getDb()).collection<any>("envios").findOne({ _id: id }, { projection: { _id: 1, estado: 1, situacion: 1, promesa: 1, historial: 1, actualizadoEn: 1 } });
    if (!doc) return NextResponse.json({ error: "No encontramos un envío con ese código." }, { status: 404 });
    const historial = (doc.historial ?? []).filter((h: any) => h.origen === "estados").map((h: any) => ({ tipo: h.tipo, ocurridoEn: h.ocurridoEn }));
    return NextResponse.json(serializar({ codigo: doc._id, estado: doc.estado, fechaEstimada: doc.promesa?.venceEn, actualizadoEn: doc.actualizadoEn, historial }));
  } catch (error) { return NextResponse.json({ error: String(error) }, { status: 503 }); }
}
