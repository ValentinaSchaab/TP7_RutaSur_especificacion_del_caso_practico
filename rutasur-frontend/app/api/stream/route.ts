import { getDb } from "@/lib/mongo";
export const dynamic = "force-dynamic";
export const runtime = "nodejs";
export async function GET(request: Request) {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let changes: ReturnType<Awaited<ReturnType<typeof getDb>>["watch"]> | undefined;
      const send = (data: unknown) => { try { controller.enqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`)); } catch {} };
      try {
        const db = await getDb();
        changes = db.watch([], { fullDocument: "updateLookup" });
        send({ connected: true });
        const heartbeat = setInterval(() => { try { controller.enqueue(encoder.encode(": ping\n\n")); } catch {} }, 15000);
        request.signal.addEventListener("abort", () => { clearInterval(heartbeat); void changes?.close(); try { controller.close(); } catch {} }, { once: true });
        for await (const change of changes) {
          if (request.signal.aborted) break;
          send({ collection: change.ns?.coll, operation: change.operationType });
        }
        clearInterval(heartbeat);
      } catch (error) {
        send({ error: String(error) });
      } finally {
        await changes?.close().catch(() => undefined);
        try { controller.close(); } catch {}
      }
    },
    cancel() {}
  });
  return new Response(stream, { headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform", Connection: "keep-alive" } });
}
