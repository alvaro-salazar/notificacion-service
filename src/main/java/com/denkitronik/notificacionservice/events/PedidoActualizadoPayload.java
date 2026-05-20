package com.denkitronik.notificacionservice.events;

public record PedidoActualizadoPayload(
    Long pedidoId,
    String estadoAnterior,
    String estadoNuevo
) {}
