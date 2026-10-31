# Frontend de RutaSur

Interfaz web básica del TP7. Es una aplicación Next.js separada del backend y consulta MongoDB mediante rutas del servidor. No consume Kafka directamente.

## Pantallas

- `/`: torre de control con estados, alertas y últimos eventos.
- `/envios/[id]`: detalle, historial, alertas y notificaciones.
- `/tracking`: consulta pública por código de envío (muestra solo información de seguimiento).
- `/analitica`: indicadores, filtros por zona, hub y fecha.
- `/demo`: botones para ejecutar escenarios e inyectar eventos de prueba.

La actualización en vivo usa Server-Sent Events con change streams de MongoDB.
