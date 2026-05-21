package com.denkitronik.notificacionservice.workers;

import com.denkitronik.notificacionservice.email.EmailService;
import com.denkitronik.notificacionservice.events.PagoConfirmadoPayload;
import com.denkitronik.notificacionservice.events.PagoRechazadoPayload;
import com.denkitronik.notificacionservice.events.PedidoActualizadoPayload;
import com.denkitronik.notificacionservice.events.PedidoCreadoPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificacionWorker {

    private final EmailService emailService;

    /**
     * Procesa PedidoCreado: envia email de confirmacion de pedido.
     * Reemplaza el Thread.sleep del codelab 17 con una llamada real a JavaMailSender.
     */
    @Async("notificacionExecutor")
    public void procesarPedidoCreado(PedidoCreadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        log.info("[WORKER] hilo={} | Enviando confirmacion de pedido #{} al cliente {}",
            hilo, payload.pedidoId(), payload.clienteId());

        String destinatario = "cliente" + payload.clienteId() + "@fogatadigital.com";

        emailService.enviar(
            destinatario,
            "Confirmacion de tu pedido #" + payload.pedidoId() + " - La Fogata Digital",
            "confirmacion-pedido",
            Map.of(
                "pedidoId",   payload.pedidoId(),
                "clienteId",  payload.clienteId(),
                "productoId", payload.productoId(),
                "cantidad",   payload.cantidad(),
                "total",      payload.total()
            )
        );
    }

    /**
     * Procesa PedidoActualizado: envia push notification.
     * Push real se implementa en el codelab 17.2 (Twilio / FCM).
     * Por ahora simula 100ms.
     */
    @Async("notificacionExecutor")
    public void procesarPedidoActualizado(PedidoActualizadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        try {
            log.info("[WORKER] hilo={} | Enviando push: pedido #{} cambio a {}",
                hilo, payload.pedidoId(), payload.estadoNuevo());
            Thread.sleep(100);
            log.info("[WORKER] hilo={} | Push enviado: pedido #{} ahora en estado {}",
                hilo, payload.pedidoId(), payload.estadoNuevo());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[WORKER] hilo={} | Push interrumpido para pedido #{}", hilo, payload.pedidoId());
        }
    }

    /**
     * Procesa PagoConfirmado: envia email de confirmacion de pago.
     */
    @Async("notificacionExecutor")
    public void procesarPagoConfirmado(PagoConfirmadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        log.info("[WORKER] hilo={} | Enviando confirmacion de pago para pedido #{}",
            hilo, payload.pedidoId());

        String destinatario = "cliente" + payload.clienteId() + "@fogatadigital.com";

        emailService.enviar(
            destinatario,
            "Pago confirmado para tu pedido #" + payload.pedidoId() + " - La Fogata Digital",
            "confirmacion-pago",
            Map.of(
                "pedidoId",    payload.pedidoId(),
                "monto",       payload.monto(),
                "metodoPago",  payload.metodoPago()
            )
        );
    }

    /**
     * Procesa PagoRechazado: notifica al cliente con instrucciones de reintento.
     */
    @Async("notificacionExecutor")
    public void procesarPagoRechazado(PagoRechazadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        log.info("[WORKER] hilo={} | Enviando notificacion de rechazo para pedido #{}",
            hilo, payload.pedidoId());

        String destinatario = "cliente" + payload.clienteId() + "@fogatadigital.com";

        emailService.enviar(
            destinatario,
            "Accion requerida: problema con el pago de tu pedido #" + payload.pedidoId(),
            "pago-rechazado",
            Map.of(
                "pedidoId", payload.pedidoId(),
                "motivo",   payload.motivo()
            )
        );
    }
}
