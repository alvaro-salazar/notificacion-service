package com.denkitronik.notificacionservice.workers;

import com.denkitronik.notificacionservice.events.PagoConfirmadoPayload;
import com.denkitronik.notificacionservice.events.PagoRechazadoPayload;
import com.denkitronik.notificacionservice.events.PedidoActualizadoPayload;
import com.denkitronik.notificacionservice.events.PedidoCreadoPayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificacionWorker {

    /**
     * Procesa el evento PedidoCreado en un hilo del pool notificacionExecutor.
     * El hilo del consumer Kafka retorna inmediatamente despues de llamar este metodo.
     *
     * Thread.sleep simula operaciones lentas reales:
     *   - 500ms: generacion de PDF de recibo (iText, JasperReports, etc.)
     *   - 200ms: llamada HTTP a SendGrid/SES para enviar el email
     */
    @Async("notificacionExecutor")
    public void procesarPedidoCreado(PedidoCreadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        try {
            log.info("[WORKER] hilo={} | Generando recibo PDF para pedido #{}...", hilo, payload.pedidoId());
            Thread.sleep(500);

            log.info("[WORKER] hilo={} | PDF listo. Enviando email a cliente{}@fogatadigital.com", hilo, payload.clienteId());
            Thread.sleep(200);

            log.info("[WORKER] hilo={} | Email enviado: pedido #{} confirmado por ${}",
                hilo, payload.pedidoId(), payload.total());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[WORKER] hilo={} | Procesamiento interrumpido para pedido #{}", hilo, payload.pedidoId());
        }
    }

    /**
     * Procesa el evento PedidoActualizado: envia push notification.
     * Simula 100ms de llamada a Firebase Cloud Messaging / APNs.
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
     * Procesa el evento PagoConfirmado: envia email de confirmacion de pago.
     * Simula 300ms de llamada HTTP al proveedor de email.
     */
    @Async("notificacionExecutor")
    public void procesarPagoConfirmado(PagoConfirmadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        try {
            log.info("[WORKER] hilo={} | Enviando confirmacion de pago para pedido #{}...",
                hilo, payload.pedidoId());
            Thread.sleep(300);

            log.info("[WORKER] hilo={} | Email enviado: ${} acreditados para pedido #{}",
                hilo, payload.monto(), payload.pedidoId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[WORKER] hilo={} | Envio interrumpido para pedido #{}", hilo, payload.pedidoId());
        }
    }

    /**
     * Procesa el evento PagoRechazado: notifica al cliente con instrucciones de reintento.
     * Simula 300ms de llamada HTTP al proveedor de email.
     */
    @Async("notificacionExecutor")
    public void procesarPagoRechazado(PagoRechazadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        try {
            log.info("[WORKER] hilo={} | Enviando notificacion de pago rechazado para pedido #{}...",
                hilo, payload.pedidoId());
            Thread.sleep(300);

            log.info("[WORKER] hilo={} | Notificacion enviada: pedido #{} -- pago rechazado",
                hilo, payload.pedidoId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[WORKER] hilo={} | Envio interrumpido para pedido #{}", hilo, payload.pedidoId());
        }
    }
}
