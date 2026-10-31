"use client";
import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { LiveBadge, Loading, Pill, useLiveRefresh } from "../../components";
export default function Detalle({ params }: { params: Promise<{ id: string }> }) {
  const [id,setId]=useState(""); const [data,setData]=useState<any>(null); const [error,setError]=useState("");
  useEffect(()=>{params.then(p=>setId(p.id));},[params]);
  const load=useCallback(async()=>{if(!id)return;try{const r=await fetch(`/api/shipments/${encodeURIComponent(id)}`,{cache:"no-store"});const j=await r.json();if(!r.ok)throw new Error(j.error);setData(j);setError("");}catch(e){setError(String(e));}},[id]);
  useEffect(()=>{void load();},[load]);useLiveRefresh(load);
  if(error)return <><Link href="/">← Volver</Link><div className="notice">{error}</div></>;
  if(!data)return <Loading/>;
  const historial=[...(data.historial??[])].sort((a:any,b:any)=>new Date(a.ocurridoEn).getTime()-new Date(b.ocurridoEn).getTime());
  return <><div className="pageHead"><div><p className="eyebrow">DETALLE DEL ENVÍO</p><h1>{data._id}</h1><p className="muted">Información e historial del envío.</p></div><LiveBadge/></div><div className="summaryGrid"><div className="panel"><span className="muted">Estado actual</span><h2><Pill>{data.estado}</Pill></h2></div><div className="panel"><span className="muted">Intentos</span><h2>{data.intentos??0}</h2></div><div className="panel"><span className="muted">Zona / Hub</span><h2>{data.zonaId??"—"} / {data.hubId??"—"}</h2></div><div className="panel"><span className="muted">Fecha prometida</span><h2 className="smallTitle">{data.promesa?.venceEn?new Date(data.promesa.venceEn).toLocaleString("es-AR"):"—"}</h2></div></div>
  <section className="panel"><h2>Historial del envío</h2><div className="timeline">{historial.map((h:any,i:number)=><div className="timelineItem" key={`${h.eventId??i}`}><span className="timelineDot"/><div><b>{h.tipo?.replaceAll("_"," ")}</b><p className="muted">{h.origen}{h.motivo?` · ${h.motivo}`:""}{h.prioridad?` · ${h.prioridad}`:""}</p><time>{h.ocurridoEn?new Date(h.ocurridoEn).toLocaleString("es-AR"):""}</time></div></div>)}</div></section>
  <div className="twoCols"><section className="panel"><h2>Alertas</h2>{(data.alertas??[]).length?(data.alertas??[]).map((a:any)=><div className="simpleRow" key={a._id}><div><b>{a.regla}</b><p className="muted">{a.causa}</p></div><Pill>{a.estado} · {a.prioridad}</Pill></div>):<p className="muted">No hay alertas para este envío.</p>}</section><section className="panel"><h2>Notificaciones</h2>{(data.notificaciones??[]).length?(data.notificaciones??[]).map((n:any)=><div className="simpleRow" key={n._id}><div><b>{n.tipo}</b><p className="muted">{n.mensaje}</p></div><time>{n.enviadaEn?new Date(n.enviadaEn).toLocaleString("es-AR"):""}</time></div>):<p className="muted">No hay notificaciones registradas.</p>}</section></div><Link href="/">← Volver a la torre de control</Link></>;
}
