package com.denkitronik.notificacionservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    /**
     * Configura el manejo de errores para todos los @KafkaListener en este servicio.
     *
     * DefaultErrorHandler: reemplaza al SeekToCurrentErrorHandler (deprecado desde Spring Kafka 2.8).
     * DeadLetterPublishingRecoverer: despues de agotar los reintentos, publica el mensaje
     *   al topic <nombre-original>.DLT con headers adicionales que identifican la excepcion.
     * FixedBackOff(2000L, 3L): reintenta 3 veces con 2 segundos de espera entre cada intento.
     *
     * @param kafkaTemplate el template usado para publicar en el DLT
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        var backoff = new FixedBackOff(2000L, 3L);
        return new DefaultErrorHandler(recoverer, backoff);
    }
}
