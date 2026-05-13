package com.denkitronik.notificacionservice.messaging;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record PedidoCreadoEvent(
        Long pedidoId,
        Long clienteId,
        String clienteNombre,
        Long productoId,
        String productoNombre,
        Integer cantidad,
        BigDecimal total,
        LocalDateTime fechaCreacion
) {}
