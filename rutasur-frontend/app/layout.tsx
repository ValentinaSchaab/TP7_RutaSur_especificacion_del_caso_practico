import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";
export const metadata: Metadata = { title: "RutaSur | Seguimiento de envíos", description: "Panel de seguimiento de envíos RutaSur" };
export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="es"><body><header className="topbar"><Link href="/" className="brand"><span className="brandMark">R</span><span>RutaSur<small>GESTIÓN DE ENVÍOS</small></span></Link><nav><Link href="/">Torre de control</Link><Link href="/tracking">Tracking público</Link><Link href="/analitica">Analítica</Link><Link href="/demo">Demo</Link></nav></header><main className="container">{children}</main><footer>RutaSur · Trabajo Práctico 7</footer></body></html>;
}
