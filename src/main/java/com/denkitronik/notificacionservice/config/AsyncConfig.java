package com.denkitronik.notificacionservice.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Configuration
@EnableAsync
public class AsyncConfig implements AsyncConfigurer {

    /**
     * Pool de hilos dedicado para los workers de notificacion.
     *
     * corePoolSize=5: siempre hay 5 hilos listos para procesar.
     * maxPoolSize=20: puede crecer hasta 20 hilos si hay picos de trabajo.
     * queueCapacity=100: hasta 100 tareas esperando en cola antes de crear mas hilos.
     * threadNamePrefix: prefijo visible en los logs para identificar los hilos del worker.
     * CallerRunsPolicy: si el pool esta lleno, el hilo que llama ejecuta la tarea el mismo
     *   en lugar de rechazarla -- evita perder trabajo a cambio de ralentizar al consumer.
     */
    @Bean(name = "notificacionExecutor")
    public Executor notificacionExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("notif-worker-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * Manejador global de excepciones para metodos @Async void.
     * Sin este handler, las excepciones en metodos @Async void se pierden silenciosamente.
     */
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (throwable, method, params) ->
            log.error("[ASYNC-ERROR] Excepcion no capturada en {}.{} con params={}: {}",
                method.getDeclaringClass().getSimpleName(),
                method.getName(),
                Arrays.toString(params),
                throwable.getMessage(),
                throwable);
    }
}
