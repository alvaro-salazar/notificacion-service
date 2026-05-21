package com.denkitronik.notificacionservice.config;

import org.springframework.context.annotation.Configuration;

/**
 * Configuracion Kafka del notificacion-service.
 *
 * En el codelab 16.1 la gestion de reintentos y DLT se configura directamente
 * en @RetryableTopic sobre @KafkaListener en NotificacionListener.
 * No se necesita un DefaultErrorHandler manual ya que @RetryableTopic
 * registra su propio NonBlockingRetriesInMemoryRecoveryStrategy.
 */
@Configuration
public class KafkaConfig {
}
