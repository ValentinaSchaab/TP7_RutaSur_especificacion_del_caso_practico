import { NextRequest, NextResponse } from "next/server";
import { getDb, serializar } from "@/lib/mongo";
export const dynamic = "force-dynamic";
export async function GET(req: NextRequest) {
  try {
    const q = req.nextUrl.searchParams.get("q")?.trim();
    const filter = q ? { _id: { $regex: q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), $options: "i" } } : {};
    const docs = await (await getDb()).collection<any>("envios").find(filter).sort({ actualizadoEn: -1 }).limit(100).toArray();
    return NextResponse.json(serializar(docs));
  } catch (error) { return NextResponse.json({ error: String(error) }, { status: 503 }); }
}
