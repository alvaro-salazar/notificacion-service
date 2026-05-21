package com.denkitronik.notificacionservice.messaging;

import com.denkitronik.notificacionservice.domain.entities.EstadoMensaje;
import com.denkitronik.notificacionservice.domain.entities.MensajeFallido;
import com.denkitronik.notificacionservice.domain.repositories.MensajeFallidoRepository;
import com.denkitronik.notificacionservice.events.PagoConfirmadoPayload;
import com.denkitronik.notificacionservice.events.PagoRechazadoPayload;
import com.denkitronik.notificacionservice.events.PedidoActualizadoPayload;
import com.denkitronik.notificacionservice.events.PedidoCreadoPayload;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificacionListener {

    private final ObjectMapper objectMapper;
    private final MensajeFallidoRepository mensajeFallidoRepository;

    /**
     * Escucha 4 topics en un solo metodo.
     *
     * @RetryableTopic: Spring Kafka gestiona reintentos y DLT automaticamente.
     *   - attempts=4: 1 intento original + 3 reintentos = 4 total
     *   - backoff 2s fijo entre intentos
     *   - topicSuffixingStrategy=SUFFIX_WITH_INDEX_VALUE: crea pedidos.creados-retry-0, -retry-1, -retry-2
     *   - Cuando se agotan los reintentos, invoca el metodo @DltHandler en esta clase.
     *
     * Con isolation.level=read_committed (codelab 16.1) este consumer
     * solo procesa mensajes cuya transaccion Kafka ya hizo commit.
     *
     * groupId = "notificacion-group" es el consumer group de este servicio.
     */
    @RetryableTopic(
        attempts = "4",
        backoff = @Backoff(delay = 2000),
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        autoCreateTopics = "true"
    )
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
     * Invocado por @RetryableTopic cuando un mensaje agota sus 4 intentos (1 original + 3 reintentos).
     *
     * Persiste el mensaje fallido en mensajes_fallidos con estado PENDIENTE
     * para que un operador pueda auditarlo o reprocesarlo.
     * El header kafka_dlt-exception-message es inyectado automaticamente
     * con el mensaje de la excepcion original.
     */
    @DltHandler
    public void onDlt(ConsumerRecord<String, String> record) {
        String errorMensaje = extractHeader(record, "kafka_dlt-exception-message");

        log.error("[DLT] Mensaje no procesable despues de 4 intentos");
        log.error("[DLT] topic={} key={} partition={} offset={}",
            record.topic(), record.key(), record.partition(), record.offset());
        log.error("[DLT] Error original: {}", errorMensaje);
        log.error("[DLT] Payload: {}", record.value());

        String eventoId = extractEventoId(record.value());

        MensajeFallido fallido = new MensajeFallido(
            eventoId,
            record.topic(),
            record.value(),
            errorMensaje,
            EstadoMensaje.PENDIENTE
        );
        mensajeFallidoRepository.save(fallido);

        log.warn("[DLT] Guardado en mensajes_fallidos: id={} eventoId={} topic={}",
            fallido.getId(), eventoId, record.topic());
    }

    private String extractHeader(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) return "desconocido";
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private String extractEventoId(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            return root.path("eventoId").asText("sin-id");
        } catch (Exception e) {
            return "sin-id";
        }
    }
}
