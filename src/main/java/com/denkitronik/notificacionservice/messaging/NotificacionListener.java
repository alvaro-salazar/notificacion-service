package com.denkitronik.notificacionservice.messaging;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class NotificacionListener {

    @RabbitListener(queues = "#{@notificacionesQueue.name}")
    public void onPedidoCreado(PedidoCreadoEvent evento) {
        log.info("====================================================");
        log.info("  NUEVA NOTIFICACION — Pedido #{}", evento.pedidoId());
        log.info("  Cliente : {} (id={})", evento.clienteNombre(), evento.clienteId());
        log.info("  Producto: {} x{}", evento.productoNombre(), evento.cantidad());
        log.info("  Total   : ${}", evento.total());
        log.info("  Fecha   : {}", evento.fechaCreacion());
        log.info("====================================================");
        simularEnvioEmail(evento);
    }

    private void simularEnvioEmail(PedidoCreadoEvent evento) {
        log.info("[EMAIL] Para: cliente{}@fogatadigital.com", evento.clienteId());
        log.info("[EMAIL] Asunto: Tu pedido #{} fue confirmado", evento.pedidoId());
        log.info("[EMAIL] Cuerpo: Hola {}, tu pedido por ${} esta en camino.",
                evento.clienteNombre(), evento.total());
    }
}
