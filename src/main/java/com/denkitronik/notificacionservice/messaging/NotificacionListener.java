package com.denkitronik.notificacionservice.messaging;

import com.denkitronik.notificacionservice.events.PagoConfirmadoPayload;
import com.denkitronik.notificacionservice.events.PagoRechazadoPayload;
import com.denkitronik.notificacionservice.events.PedidoActualizadoPayload;
import com.denkitronik.notificacionservice.events.PedidoCreadoPayload;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificacionListener {

    private final ObjectMapper objectMapper;

    /**
     * Escucha 4 topics en un solo metodo.
     * La deserializacion es manual (String -> JsonNode) para poder leer
     * el campo eventoTipo antes de saber a que clase deserializar el payload.
     *
     * groupId = "notificacion-group" es el consumer group de este servicio.
     * Si se levanta una segunda instancia con el mismo groupId,
     * Kafka distribuye las particiones entre ambas (load balancing).
     */
    @KafkaListener(
        topics = {"pedidos.creados", "pedidos.actualizados", "pagos.confirmados", "pagos.rechazados"},
        groupId = "notificacion-group"
    )
    public void onEvento(ConsumerRecord<String, String> record) throws Exception {
        log.debug("[NOTIF] Recibido: topic={} key={} offset={}",
            record.topic(), record.key(), record.offset());

        JsonNode root = objectMapper.readTree(record.value());
        String tipo = root.path("eventoTipo").asText();
        JsonNode payload = root.path("payload");

        switch (tipo) {
            case "PedidoCreado" -> {
                var p = objectMapper.treeToValue(payload, PedidoCreadoPayload.class);
                log.info("[NOTIF] Nuevo pedido #{} | Cliente {} | Total ${}",
                    p.pedidoId(), p.clienteId(), p.total());
                log.info("[EMAIL] Para: cliente{}@fogatadigital.com | Pedido #{} confirmado por ${}",
                    p.clienteId(), p.pedidoId(), p.total());
            }
            case "PedidoActualizado" -> {
                var p = objectMapper.treeToValue(payload, PedidoActualizadoPayload.class);
                log.info("[NOTIF] Pedido #{} cambio de estado: {} -> {}",
                    p.pedidoId(), p.estadoAnterior(), p.estadoNuevo());
                log.info("[PUSH] Tu pedido #{} esta ahora en estado: {}",
                    p.pedidoId(), p.estadoNuevo());
            }
            case "PagoConfirmado" -> {
                var p = objectMapper.treeToValue(payload, PagoConfirmadoPayload.class);
                log.info("[NOTIF] Pago confirmado | Pedido #{} | Monto ${}",
                    p.pedidoId(), p.monto());
                log.info("[EMAIL] Tu pago de ${} fue acreditado para el pedido #{}",
                    p.monto(), p.pedidoId());
            }
            case "PagoRechazado" -> {
                var p = objectMapper.treeToValue(payload, PagoRechazadoPayload.class);
                log.info("[NOTIF] Pago rechazado | Pedido #{}", p.pedidoId());
                log.info("[EMAIL] Tu pago para el pedido #{} fue rechazado. Intenta con otro metodo.",
                    p.pedidoId());
            }
            default ->
                log.warn("[NOTIF] Tipo de evento desconocido: {} en topic {}", tipo, record.topic());
        }
    }

    /**
     * Manejador del Dead Letter Topic.
     * Se invoca automaticamente cuando un mensaje agota sus reintentos (3 intentos, 2s entre cada uno).
     * En produccion: guardar en BD de auditoria, enviar alerta a PagerDuty/Slack, etc.
     */
    @DltHandler
    public void onDlt(ConsumerRecord<String, String> record) {
        log.error("[DLT] Mensaje no procesable despues de 3 reintentos");
        log.error("[DLT] topic={} key={} partition={} offset={}",
            record.topic(), record.key(), record.partition(), record.offset());
        log.error("[DLT] Payload: {}", record.value());
        // TODO produccion: insertar en tabla 'mensajes_fallidos' con topic, key, payload, timestamp
        // TODO produccion: enviar alerta a canal de alertas
    }
}
