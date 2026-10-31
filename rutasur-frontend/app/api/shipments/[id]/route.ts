import { NextRequest, NextResponse } from "next/server";
import { getDb, serializar } from "@/lib/mongo";

export const dynamic = "force-dynamic";

export async function GET(
  _req: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await context.params;
    const db = await getDb();

    const [envio, alertas, notificaciones] = await Promise.all([
      db.collection<any>("envios").findOne({ _id: id }),
      db.collection("alertas").find({ shipmentId: id }).sort({ abiertaEn: -1 }).toArray(),
      db.collection("notificaciones").find({ shipmentId: id }).sort({ enviadaEn: -1 }).toArray()
    ]);

    if (!envio) {
      return NextResponse.json(
        { error: "No se encontro el envio" },
        { status: 404 }
      );
    }

    return NextResponse.json(
      serializar({ ...envio, alertas, notificaciones })
    );
  } catch (error) {
    return NextResponse.json(
      { error: String(error) },
      { status: 503 }
    );
  }
}