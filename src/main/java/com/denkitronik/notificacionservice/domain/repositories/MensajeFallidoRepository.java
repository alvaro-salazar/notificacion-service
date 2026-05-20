package com.denkitronik.notificacionservice.domain.repositories;

import com.denkitronik.notificacionservice.domain.entities.MensajeFallido;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MensajeFallidoRepository extends JpaRepository<MensajeFallido, Long> {
}
