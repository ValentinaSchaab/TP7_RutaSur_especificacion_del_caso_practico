import { NextResponse } from "next/server";
import { getDb, serializar } from "@/lib/mongo";

export const dynamic = "force-dynamic";
export async function GET() {
  try {
    const db = await getDb();
    const envios = db.collection("envios");
    const alertas = db.collection("alertas");
    const [porEstado, alertasAbiertas, ultimos, total] = await Promise.all([
      envios.aggregate([{ $group: { _id: "$estado", cantidad: { $sum: 1 } } }, { $sort: { _id: 1 } }]).toArray(),
      alertas.find({ estado: "ABIERTA" }).sort({ prioridad: 1, abiertaEn: -1 }).limit(12).toArray(),
      envios.aggregate([
        { $unwind: "$historial" },
        { $sort: { "historial.ocurridoEn": -1 } },
        { $limit: 12 },
        { $project: { _id: 0, shipmentId: "$_id", estado: "$historial.tipo", origen: "$historial.origen", ocurridoEn: "$historial.ocurridoEn", motivo: "$historial.motivo" } }
      ]).toArray(),
      envios.countDocuments()
    ]);
    return NextResponse.json(serializar({ total, porEstado, alertasAbiertas, ultimos }));
  } catch (error) {
    return NextResponse.json({ error: "No se pudo consultar MongoDB", detalle: String(error) }, { status: 503 });
  }
}
