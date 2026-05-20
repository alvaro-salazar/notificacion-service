package com.denkitronik.notificacionservice.messaging;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer group independiente -- simula un servicio de analytics.
 * Escucha los mismos topics que NotificacionListener pero con groupId diferente.
 * Kafka le entrega TODOS los mensajes de forma independiente -- no compite con notificacion-group.
 *
 * En produccion: este seria un analytics-service separado con su propio deployment.
 * Lo incluimos aqui para demostrar el broadcasting sin necesitar un quinto servicio.
 */
@Slf4j
@Component
public class AnalyticsListener {

    @KafkaListener(
        topics = {"pedidos.creados", "pagos.confirmados"},
        groupId = "analytics-group"
    )
    public void onEvento(ConsumerRecord<String, String> record) {
        log.info("[ANALYTICS] topic={} key={} partition={} offset={}",
            record.topic(), record.key(), record.partition(), record.offset());
        // En produccion: enviar a BigQuery, ClickHouse, Redshift, etc.
        // En produccion: incrementar contador en Prometheus, actualizar dashboard en Grafana
    }
}
