package com.denkitronik.notificacionservice.events;

import java.math.BigDecimal;

public record PagoConfirmadoPayload(
    Long pagoId,
    Long pedidoId,
    BigDecimal monto
) {}
