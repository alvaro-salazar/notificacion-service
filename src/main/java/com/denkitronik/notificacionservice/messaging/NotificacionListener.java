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
     * La deserializacion es manual (String -> JsonNode) para poder leer
     * el campo eventoTipo antes de saber a que clase deserializar el payload.
     *
     * Con isolation.level=read_committed (codelab 16.1) este consumer
     * solo procesa mensajes cuya transaccion Kafka ya hizo commit.
     * Mensajes de productores abortados son invisibles.
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
     *
     * En codelab 16.1 persiste el mensaje fallido en la tabla 'mensajes_fallidos'
     * con estado PENDIENTE para que un operador pueda auditarlo o reprocesarlo.
     * El header kafka_dlt-exception-message es inyectado automaticamente por
     * DeadLetterPublishingRecoverer con el mensaje de la excepcion original.
     */
    @DltHandler
    public void onDlt(ConsumerRecord<String, String> record) {
        String errorMensaje = extractHeader(record, "kafka_dlt-exception-message");

        log.error("[DLT] Mensaje no procesable despues de 3 reintentos");
        log.error("[DLT] topic={} key={} partition={} offset={}",
            record.topic(), record.key(), record.partition(), record.offset());
        log.error("[DLT] Error original: {}", errorMensaje);
        log.error("[DLT] Payload: {}", record.value());

        // Extraer eventoId del envelope para correlacion
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
