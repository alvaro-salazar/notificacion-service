package com.denkitronik.notificacionservice.email;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.util.Map;

/**
 * Servicio de email transaccional.
 *
 * Usa JavaMailSender (Spring Boot Starter Mail) para construir mensajes MIME
 * y TemplateEngine (Thymeleaf) para renderizar el HTML del cuerpo del email.
 *
 * En desarrollo: apunta a MailHog (localhost:1025), que captura todos los emails.
 * En produccion: cambiar MAIL_HOST/PORT por el relay SMTP de SendGrid o AWS SES.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EmailService {

    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;

    @Value("${notificacion.mail.from}")
    private String from;

    /**
     * Envia un email HTML usando una plantilla Thymeleaf.
     *
     * @param to       destinatario (ej: "cliente1@fogatadigital.com")
     * @param subject  asunto del email
     * @param template nombre del template en templates/email/ (sin .html)
     * @param vars     variables que el template puede usar con th:text="${nombre}"
     */
    public void enviar(String to, String subject, String template, Map<String, Object> vars) {
        try {
            // 1. Renderizar el HTML con Thymeleaf
            Context ctx = new Context();
            ctx.setVariables(vars);
            String html = templateEngine.process("email/" + template, ctx);

            // 2. Construir el MimeMessage (soporte HTML + charset UTF-8)
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
            helper.setFrom(from);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(html, true); // true = es HTML

            // 3. Enviar
            mailSender.send(message);
            log.info("[EMAIL] Enviado | to={} | subject={}", to, subject);

        } catch (MessagingException e) {
            // No relanzamos para no bloquear el worker; el error queda en log
            // y el AsyncUncaughtExceptionHandler no aplica aqui porque capturamos
            log.error("[EMAIL] Error enviando a {} | subject={} | error={}", to, subject, e.getMessage(), e);
        }
    }
}
