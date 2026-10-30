#!/bin/bash
# Crea los 6 topics del catálogo de eventos (3 particiones cada uno).
set -e
BOOTSTRAP="kafka:19092"
KT="/opt/kafka/bin/kafka-topics.sh"
D30=2592000000   # 30 días
D7=604800000     # 7 días

crear() { # nombre retención|compact
  local cfg="retention.ms=$2"
  [ "$2" = "compact" ] && cfg="cleanup.policy=compact"
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$1" --partitions 3 --replication-factor 1 --config "$cfg"
}

crear envios.estados        $D30
crear envios.promesas       compact   # compactado: queda la última promesa de cada envío
crear envios.reprogramaciones $D30
crear alertas.sla           $D7
crear notificaciones.enviadas $D7
crear envios.estados.dlq    $D30

echo "Topics creados:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
