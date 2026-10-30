# RutaSur — seguimiento de envíos orientado a eventos

Plataforma de seguimiento de envíos en tiempo real para un operador de última milla ficticio.
**Hipótesis:** si cada cambio de estado de un envío es un evento, la empresa puede intervenir durante la
operación en lugar de analizar fracasos al día siguiente.

```
Simulador ──▶ Kafka (6 topics) ──▶ Proyector · Notificaciones · Reprogramación · Monitor de SLA · Analítica
                                              │ (tres de ellos publican eventos propios)
                                              ▼
                                   MongoDB (vistas de lectura) ──▶ Next.js (etapa 2)
```
Kafka es el único punto de contacto entre servicios: ninguno llama a otro. Cada servicio es un proceso
Node.js separado y un consumer group independiente.

## Requisitos
Docker Desktop abierto. Para correr las pruebas automáticas: Node.js 20+ y pnpm.

## Arranque (un solo comando)

> Si tenés levantado otro proyecto con Kafka/Mongo (por ejemplo el TP6), apagalo antes: `docker compose down`
> en esa carpeta. Usan los mismos puertos (9092, 27017, 8080).

```powershell
docker compose up -d --build
docker compose ps -a
```
La primera vez tarda varios minutos (construye la imagen). El simulador empieza solo el flujo continuo.

| Qué | Dónde |
|---|---|
| Kafka UI (topics, mensajes, lag, DLQ) | http://localhost:8080 |
| API de control del simulador | http://localhost:4000/status |
| MongoDB | `mongodb://localhost:27017/rutasur?directConnection=true` |

Para **repetir la demo desde cero**: `docker compose down -v` (borra Kafka y Mongo) y volver a levantar.
Reiniciar solo el simulador reinicia su reloj simulado, mientras que el monitor de SLA conserva el suyo: no lo hagas
en medio de una demo.

## Catálogo de eventos
| Topic | Produce | Consume (grupo) | Retención |
|---|---|---|---|
| `envios.estados` | simulador | proyector, monitor-sla, notificaciones, reprogramacion, analitica | 30 días |
| `envios.promesas` | simulador | monitor-sla | compactado |
| `envios.reprogramaciones` | reprogramación | proyector | 30 días |
| `alertas.sla` | monitor de SLA | proyector, notificaciones | 7 días |
| `notificaciones.enviadas` | notificaciones | proyector | 7 días |
| `envios.estados.dlq` | cualquier consumidor | revisión manual | 30 días |

Clave de partición: siempre `shipmentId` (orden por envío). `envios.estados` tiene 3 particiones.

## Dónde está cada cosa
```
src/lib/          esquemas (zod), bus Kafka (kafkajs, aislado en una clase), consumidor con idempotencia/reintentos/DLQ, tiempo simulado
src/services/     proyector, notificaciones, reprogramacion, monitor-sla, analitica, analitica-tiempos (4.º indicador)
src/simulador/    generador con semilla fija + API de control
src/tools/        recuento.ts: recálculo independiente de los indicadores
test/e2e.js       prueba de punta a punta (26 verificaciones)
```

## Cómo se cumplen las decisiones de diseño
- **Al menos una vez + idempotencia:** el `eventId` se guarda en `eventos_procesados` (índice único
  `consumidor + eventId`, TTL 7 días) **en la misma transacción de MongoDB** que el efecto. Los eventos que un
  servicio publica en respuesta usan un `eventId` determinístico, así un reproceso no genera duplicados.
- **Commit de offsets manual**, después de escribir en MongoDB (`autoCommit: false`).
- **JSON con validación de esquema** (zod) en cada consumidor. Un evento inválido va a `envios.estados.dlq` con el motivo
  en los headers (`x-error`, `x-topic-origen`, `x-particion`, `x-offset`, `x-grupo`). Un error técnico se reintenta 3 veces
  (1 s, 2 s, 4 s) y luego va a la DLQ. El proyector es el que envía los inválidos a la DLQ (los demás los omiten con un
  aviso en el log), para no multiplicar copias del mismo mensaje.
- **Monitor de SLA:** consumidor con estado en memoria respaldado en MongoDB (`sla_estado`, `sla_emitidas`, `sla_reloj`).
  Usa el **reloj simulado que viaja en los eventos** (entre eventos avanza con el factor de aceleración) y revisa todos
  los envíos abiertos cada minuto (`SLA_TICK_MS`, 60 000 por defecto).
- **MongoDB es una vista:** el historial va embebido en `envios`; `metricas_diarias` se actualiza con `$inc` atómicos.
- **Desviaciones menores respecto del documento** (decisiones propias, para poder justificarlas):
  - Los eventos de `envios.estados` llevan también `promesaVenceEn` (desnormalizado) para que el proyector y la analítica
    no tengan que cruzar dos topics; el monitor de SLA sí consume `envios.promesas`, como indica el catálogo.
  - `ZONA_INACCESIBLE` no tiene regla en el documento: se reprograma al día hábil siguiente en la misma franja.
  - Todas las colecciones viven en una sola base (`rutasur`), porque Next.js lee de ahí.

## Probar el sistema
### Disparar los escenarios (desde PowerShell)
```powershell
Invoke-RestMethod -Method Post http://localhost:4000/scenarios/feliz
Invoke-RestMethod -Method Post http://localhost:4000/scenarios/ausente
Invoke-RestMethod -Method Post http://localhost:4000/scenarios/demorado
Invoke-RestMethod -Method Post http://localhost:4000/scenarios/tercer-fallo
Invoke-RestMethod -Method Post http://localhost:4000/scenarios/rafaga      # 500 envíos
Invoke-RestMethod -Method Post http://localhost:4000/inject/malformado      # evento inválido -> DLQ
```
La respuesta muestra el id del primer envío (`DEMO-FELIZ-XXXX-01`). Para verlo en MongoDB:
```powershell
docker compose exec mongo mongo rutasur --quiet --eval 'db.envios.find({_id:/DEMO-FELIZ/}).pretty()'
docker compose exec mongo mongo rutasur --quiet --eval 'db.alertas.find({estado:"ABIERTA"}).sort({prioridad:1}).limit(5).pretty()'
```
### Lag de un grupo de consumidores
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group notificaciones
```
### Caída de un servicio
```powershell
docker compose stop notificaciones     # el tracking y las alertas siguen; el lag del grupo crece
docker compose start notificaciones    # procesa lo pendiente sin duplicar avisos
```
### Indicadores vs. recuento manual
```powershell
docker compose run --rm proyector node dist/tools/recuento.js
```
### 4.º indicador y reprocesamiento
```powershell
docker compose --profile reproceso up -d analitica-tiempos   # grupo NUEVO: lee envios.estados desde el inicio
docker compose exec mongo mongo rutasur --quiet --eval 'db.metricas_diarias.aggregate([{$group:{_id:"$hubId",n:{$sum:"$tiempoHub.cantidad"},ms:{$sum:"$tiempoHub.sumaMs"}}}])'
```
### Reconstruir `envios` desde Kafka
```powershell
docker compose stop proyector
docker compose exec mongo mongo rutasur --quiet --eval 'db.envios.drop(); db.alertas.drop(); db.notificaciones.drop(); db.eventos_procesados.deleteMany({consumidor:"proyector"})'
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group proyector --reset-offsets --to-earliest --all-topics --execute
docker compose start proyector
```
(Hay que borrar también `eventos_procesados` del proyector; si no, los eventos se descartarían como ya aplicados.)

### Pruebas automáticas
Con Mongo arriba (`docker compose up -d mongo`), en la carpeta del proyecto:
```powershell
pnpm install
pnpm run build
pnpm test
```
Levanta los servicios reales contra MongoDB real con un bus en memoria y verifica los criterios de aceptación.
Solo usa la base `rutasur_test`.

## Parámetros del simulador (variables de entorno)
`SIM_SEED` (42), `SIM_ENVIOS_POR_MINUTO` (20), `SIM_FACTOR` (120 → 1 hora simulada = 30 s),
`SIM_P_DEMORA_HUB` (0.10), `SIM_P_DUPLICADO` (0.02), `SIM_P_FUERA_DE_ORDEN` (0.02), `SIM_P_MALFORMADO` (0.01).
Las 6 zonas tienen tasas de fallo de 5 % a 30 %; los motivos se distribuyen 60/20/15/5.
