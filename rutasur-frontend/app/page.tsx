"use client";
import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { LiveBadge, Loading, Pill, useLiveRefresh } from "./components";
type Data = { total: number; porEstado: { _id: string; cantidad: number }[]; alertasAbiertas: any[]; ultimos: any[] };
export default function Home() {
  const [data, setData] = useState<Data | null>(null); const [error, setError] = useState("");
  const load = useCallback(async () => { try { const r = await fetch("/api/overview", { cache: "no-store" }); const j = await r.json(); if (!r.ok) throw new Error(j.detalle || j.error); setData(j); setError(""); } catch (e) { setError(String(e)); } }, []);
  useEffect(() => { void load(); }, [load]); useLiveRefresh(load);
  return <><div className="pageHead"><div><p className="eyebrow">OPERACIONES</p><h1>Torre de control</h1><p className="muted">Resumen general del estado de los envíos.</p></div><LiveBadge /></div>
    {error && <div className="notice">{error}. Verificá que MongoDB esté funcionando.</div>}
    {!data ? <Loading /> : <>
      <div className="stats"><div className="stat"><span>Total de envíos</span><strong>{data.total}</strong><small>Registrados en el sistema</small></div>{["CREADO", "INGRESADO_HUB", "CLASIFICADO", "EN_REPARTO", "ENTREGADO", "ENTREGA_FALLIDA"].map(s => <div className="stat" key={s}><span>{s.replaceAll("_", " ")}</span><strong>{data.porEstado.find(x => x._id === s)?.cantidad ?? 0}</strong><small>Envíos</small></div>)}</div>
      <div className="twoCols"><section className="panel"><div className="sectionHead"><h2>Alertas abiertas</h2><span className="counter">{data.alertasAbiertas.length}</span></div>{data.alertasAbiertas.length ? <div className="tableWrap"><table><thead><tr><th>Envío</th><th>Regla</th><th>Prioridad</th><th>Zona</th></tr></thead><tbody>{data.alertasAbiertas.map(a => <tr key={a._id}><td><Link href={`/envios/${encodeURIComponent(a.shipmentId)}`}>{a.shipmentId}</Link></td><td>{a.regla}</td><td><Pill>{a.prioridad}</Pill></td><td>{a.zonaId ?? "—"}</td></tr>)}</tbody></table></div> : <div className="empty small">No hay alertas abiertas.</div>}</section>
      <section className="panel"><div className="sectionHead"><h2>Últimos eventos</h2><Link href="/tracking">Consultar envío →</Link></div>{data.ultimos.length ? <div className="eventList">{data.ultimos.map((e,i) => <div className="event" key={`${e.shipmentId}-${e.ocurridoEn}-${i}`}><span className="eventDot"/><div><b>{e.estado?.replaceAll("_", " ")}</b><p><Link href={`/envios/${encodeURIComponent(e.shipmentId)}`}>{e.shipmentId}</Link>{e.motivo ? ` · ${e.motivo}` : ""}</p></div><time>{e.ocurridoEn ? new Date(e.ocurridoEn).toLocaleString("es-AR") : ""}</time></div>)}</div> : <div className="empty small">Todavía no hay eventos.</div>}</section></div>
      <section className="panel"><div className="sectionHead"><h2>Envíos recientes</h2><Link href="/tracking">Buscar un envío →</Link></div><RecentShipments /></section>
    </>}
  </>;
}
function RecentShipments() { const [items,setItems]=useState<any[]>([]); useEffect(()=>{fetch("/api/shipments").then(r=>r.json()).then(setItems).catch(()=>setItems([]));},[]); return <div className="tableWrap"><table><thead><tr><th>Identificador</th><th>Estado</th><th>Zona</th><th>Hub</th><th>Actualizado</th></tr></thead><tbody>{items.slice(0,8).map(e=><tr key={e._id}><td><Link href={`/envios/${encodeURIComponent(e._id)}`}>{e._id}</Link></td><td><Pill>{e.estado}</Pill></td><td>{e.zonaId??"—"}</td><td>{e.hubId??"—"}</td><td>{e.actualizadoEn?new Date(e.actualizadoEn).toLocaleString("es-AR"):"—"}</td></tr>)}</tbody></table></div>; }
