package com.denkitronik.notificacionservice.events;

import java.time.Instant;

/**
 * Envelope estandar para todos los eventos de integracion del sistema.
 * El consumer deserializa primero este wrapper, lee eventoTipo,
 * y luego deserializa el payload al tipo concreto correspondiente.
 */
public record EventoBase<T>(
    String eventoId,
    String eventoTipo,
    String version,
    Instant ocurrioEn,
    String servicioOrigen,
    T payload
) {}
