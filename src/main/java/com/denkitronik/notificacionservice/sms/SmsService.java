package com.denkitronik.notificacionservice.sms;

import com.twilio.exception.ApiException;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Servicio de SMS transaccional via Twilio REST API.
 *
 * Twilio usa HTTP (no SMTP): cada llamada a create() realiza una peticion
 * POST a https://api.twilio.com/2010-04-01/Accounts/{SID}/Messages.json
 *
 * En produccion, "to" debe ser el numero real del cliente (ej: +573001234567).
 * Para pruebas locales, usa el numero magico +15005550006 que Twilio acepta
 * sin enviar SMS reales.
 */
@Slf4j
@Service
public class SmsService {

    @Value("${twilio.from-number}")
    private String fromNumber;

    /**
     * Envia un SMS al numero indicado.
     *
     * @param to    numero destino en formato E.164 (ej: "+573001234567")
     * @param texto cuerpo del mensaje (max 160 caracteres para un SMS simple)
     */
    public void enviarSms(String to, String texto) {
        try {
            Message message = Message.creator(
                    new PhoneNumber(to),
                    new PhoneNumber(fromNumber),
                    texto
            ).create();

            log.info("[SMS] Enviado | sid={} | to={} | status={}",
                    message.getSid(), to, message.getStatus());

        } catch (ApiException e) {
            // ApiException incluye el codigo de error de Twilio (ver: https://www.twilio.com/docs/api/errors)
            // Codigo 21211: numero destino invalido
            // Codigo 21408: permiso denegado para enviar a ese pais
            // Codigo 21614: numero no es SMS-capable
            log.error("[SMS] Error enviando a {} | twilio-code={} | error={}",
                    to, e.getCode(), e.getMessage(), e);
        }
    }
}
