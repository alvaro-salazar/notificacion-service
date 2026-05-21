package com.denkitronik.notificacionservice.workers;

import com.denkitronik.notificacionservice.email.EmailService;
import com.denkitronik.notificacionservice.events.PagoConfirmadoPayload;
import com.denkitronik.notificacionservice.events.PagoRechazadoPayload;
import com.denkitronik.notificacionservice.events.PedidoActualizadoPayload;
import com.denkitronik.notificacionservice.events.PedidoCreadoPayload;
import com.denkitronik.notificacionservice.sms.SmsService;
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
    private final SmsService smsService;

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
     * Procesa PedidoActualizado: envia SMS al cliente con el nuevo estado del pedido.
     * Reemplaza el Thread.sleep del codelab 17 con una llamada real a Twilio.
     *
     * NOTA: En produccion, el numero de telefono del cliente debe venir de
     * cliente-service (lookup por clienteId) o incluirse en el evento.
     * Aqui usamos un numero demo derivado del clienteId para simplificar.
     */
    @Async("notificacionExecutor")
    public void procesarPedidoActualizado(PedidoActualizadoPayload payload) {
        String hilo = Thread.currentThread().getName();
        log.info("[WORKER] hilo={} | Enviando SMS de estado: pedido #{} -> {}",
            hilo, payload.pedidoId(), payload.estadoNuevo());

        // Demo: numero derivado del clienteId. En produccion: consultar cliente-service.
        String telefono = "+15005550006"; // numero magico de pruebas de Twilio

        String texto = String.format(
            "La Fogata Digital: Tu pedido #%d ha cambiado a estado %s. " +
            "Gracias por tu compra.",
            payload.pedidoId(), payload.estadoNuevo()
        );

        smsService.enviarSms(telefono, texto);
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
