package com.denkitronik.notificacionservice.sms;

import com.twilio.Twilio;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Inicializa el cliente global de Twilio con las credenciales de la cuenta.
 *
 * Twilio.init() es un metodo estatico que configura el AccountSid y AuthToken
 * a nivel de JVM. Solo necesita llamarse una vez al arrancar la aplicacion.
 *
 * En modo pruebas (con credenciales de test de Twilio), los mensajes se
 * "envian" correctamente (retornan un SID) pero no llegan a ningun telefono real.
 * Ver: https://www.twilio.com/docs/iam/test-credentials
 */
@Slf4j
@Configuration
public class TwilioConfig {

    @Value("${twilio.account-sid}")
    private String accountSid;

    @Value("${twilio.auth-token}")
    private String authToken;

    @PostConstruct
    public void init() {
        Twilio.init(accountSid, authToken);
        // Loguear solo los primeros 8 caracteres del SID para no exponer credenciales
        log.info("[TWILIO] Inicializado | accountSid={}...", accountSid.substring(0, Math.min(8, accountSid.length())));
    }
}
