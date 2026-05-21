package com.denkitronik.notificacionservice.events;

public record PedidoActualizadoPayload(
    Long pedidoId,
    Long clienteId,
    String estadoAnterior,
    String estadoNuevo
) {}
