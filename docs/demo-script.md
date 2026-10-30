# Guion de la demo (≈ 15 minutos)

Pantalla dividida: Kafka UI (http://localhost:8080) y la torre de control (etapa 2) o la terminal.

| # | Paso | Qué se hace | Qué se ve | Concepto |
|---|---|---|---|---|
| 1 | Arranque | `docker compose up -d --build` | El simulador publica solo; en Kafka UI los topics crecen | Log de eventos |
| 2 | Envío feliz | `POST /scenarios/feliz` | Un envío recorre los 5 estados hasta ENTREGADO | Productor, consumidor, proyección |
| 3 | Ausente | `POST /scenarios/ausente` | Casi a la vez: aviso (`notificaciones.enviadas`), reprogramación (`envios.reprogramaciones`) y +1 en analítica | Tres grupos sobre un mismo evento |
| 4 | Demorado | `POST /scenarios/demorado` | Alerta MEDIA al instante, ALTA a los ~60 s, CRÍTICA a los ~3 min; se cierra al entregarse | Procesamiento con estado y tiempo |
| 5 | Caída de un servicio | `docker compose stop notificaciones`, crear fallos, ver el lag, `start` | El lag crece, al volver se procesa sin duplicar | Aislamiento, offsets, idempotencia |
| 6 | Evento inválido | `POST /inject/malformado` | Aparece en `envios.estados.dlq` con el motivo; el flujo sigue | Manejo de errores |
| 7 | Analítica | Consultar `metricas_diarias` / panel (etapa 2) | La zona y la franja con más fallos (Z6 por la tarde) → mejora: franja vespertina | Agregados incrementales |
| 8 | Reprocesamiento | `docker compose --profile reproceso up -d analitica-tiempos` | El 4.º indicador se completa con datos anteriores a su creación | Retención y replay |

Los pasos 5 y 8 son los que distinguen esta solución de una cola tradicional o de llamadas REST encadenadas.

## Extras
- **Ráfaga:** `POST /scenarios/rafaga` → 500 envíos en pocos segundos; mirar el lag en Kafka UI y cómo se recupera.
- **Idempotencia:** `POST /inject/duplicado` reenvía el último evento: no agrega una entrada repetida al historial
  (el log del proyector dice "Evento duplicado descartado").
- **Logs por evento:** `docker compose logs -f proyector` muestra `eventId`, partición y offset de cada evento.
