package com.denkitronik.notificacionservice.events;

public record PagoRechazadoPayload(
    Long pagoId,
    Long pedidoId
) {}
