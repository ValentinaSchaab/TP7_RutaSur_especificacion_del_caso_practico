"use client";
import { useEffect, useState } from "react";
export function LiveBadge() { const [ok, setOk] = useState(false); useEffect(() => { const es = new EventSource("/api/stream"); es.onopen = () => setOk(true); es.onmessage = () => setOk(true); es.onerror = () => setOk(false); return () => es.close(); }, []); return <span className={`live ${ok ? "liveOn" : ""}`}><i />{ok ? "Actualización en vivo" : "Conectando…"}</span>; }
export function useLiveRefresh(refresh: () => void) { useEffect(() => { const es = new EventSource("/api/stream"); es.onmessage = () => refresh(); return () => es.close(); }, [refresh]); }
export function Pill({ children }: { children: React.ReactNode }) { const s = String(children ?? "").toLowerCase(); const cls = s.includes("entreg") || s.includes("cerrad") || s.includes("reprogramado") ? "pill green" : s.includes("critic") || s.includes("fall") || s.includes("vencid") ? "pill red" : s.includes("alta") || s.includes("reparto") ? "pill orange" : "pill blue"; return <span className={cls}>{children || "—"}</span>; }
export function Loading({ text = "Cargando información…" }: { text?: string }) { return <div className="empty">{text}</div>; }
