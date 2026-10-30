# TP7 - RutaSur

## Sobre el proyecto

El objetivo es poder hacer un seguimiento de los envíos y controlar qué pasa con cada uno durante todo su recorrido, desde que se crea hasta que se entrega.

También se tienen en cuenta las situaciones en las que una entrega falla, hay demoras o es necesario volver a programar un intento de entrega.

## ¿Cómo funciona el sistema?

El proyecto está dividido en varios servicios. Cada uno tiene una función diferente y se comunican entre ellos utilizando Kafka.

Los servicios que forman parte del proyecto son:

- **Simulador:** genera los envíos y los eventos que van ocurriendo.
- **Proyector:** recibe los eventos y actualiza la información de los envíos en MongoDB.
- **Notificaciones:** se encarga de generar avisos cuando ocurre algún problema o situación importante.
- **Reprogramación:** se ocupa de organizar un nuevo intento de entrega cuando una entrega falla.
- **Monitor SLA:** controla los tiempos de entrega y genera alertas cuando se detectan demoras.
- **Analítica:** obtiene información de los envíos y calcula distintos indicadores.

La información se guarda en MongoDB y Kafka se utiliza para que los servicios puedan comunicarse mediante eventos.

## Escenarios del simulador

Para probar el funcionamiento del sistema se pueden ejecutar distintos escenarios:

- **Feliz:** el envío realiza todo el recorrido y llega correctamente al destinatario.
- **Ausente:** el destinatario no se encuentra y se debe reprogramar la entrega.
- **Demorado:** el envío presenta una demora y se pueden generar alertas por los tiempos de entrega.
- **Tercer fallo:** el envío tiene tres intentos fallidos y se marca para devolución.
- **Ráfaga:** genera varios envíos para probar el funcionamiento del sistema con más eventos.

También se realizan pruebas con eventos duplicados, eventos fuera de orden y mensajes incorrectos.

## Pruebas realizadas

Se realizaron pruebas para verificar que los distintos servicios funcionen correctamente y que se cumplan los casos planteados en el trabajo práctico.

Se probaron los recorridos de los envíos, las entregas fallidas, las reprogramaciones, las notificaciones, las alertas SLA, los indicadores y el reprocesamiento de eventos.

Al ejecutar las pruebas automáticas se obtuvieron 26 verificaciones correctas.
