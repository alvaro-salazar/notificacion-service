package com.denkitronik.notificacionservice.domain.entities;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "mensajes_fallidos")
@Getter
@Setter
@NoArgsConstructor
public class MensajeFallido {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "evento_id", nullable = false)
    private String eventoId;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "error_mensaje")
    private String errorMensaje;

    @Enumerated(EnumType.STRING)
    @Column(name = "estado", nullable = false)
    private EstadoMensaje estado;

    @Column(name = "creado_en", nullable = false)
    private Instant creadoEn;

    public MensajeFallido(String eventoId, String topic, String payload,
                          String errorMensaje, EstadoMensaje estado) {
        this.eventoId = eventoId;
        this.topic = topic;
        this.payload = payload;
        this.errorMensaje = errorMensaje;
        this.estado = estado;
        this.creadoEn = Instant.now();
    }
}
