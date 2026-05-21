package com.denkitronik.notificacionservice.events;

import java.math.BigDecimal;

public record PagoConfirmadoPayload(
    Long pagoId,
    Long pedidoId,
    Long clienteId,
    BigDecimal monto,
    String metodoPago
) {}
