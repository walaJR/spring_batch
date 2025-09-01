package com.batch.SpringBatchApp.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.batch.SpringBatchApp.entities.CuentasAnuales;
import com.batch.SpringBatchApp.entities.Intereses;
import com.batch.SpringBatchApp.entities.Transacciones;
import com.batch.SpringBatchApp.steps.CuentasAnualesItemProcessor;
import com.batch.SpringBatchApp.steps.CuentasAnualesItemReader;
import com.batch.SpringBatchApp.steps.CuentasAnualesItemWriter;
import com.batch.SpringBatchApp.steps.InteresesItemProcessor;
import com.batch.SpringBatchApp.steps.InteresesItemReader;
import com.batch.SpringBatchApp.steps.InteresesItemWriter;
import com.batch.SpringBatchApp.steps.TransaccionesItemProcessor;
import com.batch.SpringBatchApp.steps.TransaccionesItemReader;
import com.batch.SpringBatchApp.steps.TransaccionesItemWriter;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableBatchProcessing
@Slf4j
public class BatchConfig {

    @Autowired
    private TransaccionesItemReader transaccionesItemReader;

    @Autowired
    private TransaccionesItemProcessor transaccionesItemProcessor;

    @Autowired
    private TransaccionesItemWriter transaccionesItemWriter;

    @Autowired
    private InteresesItemReader interesesItemReader;

    @Autowired
    private InteresesItemProcessor interesesItemProcessor;

    @Autowired
    private InteresesItemWriter interesesItemWriter;

    @Autowired
    private CuentasAnualesItemReader cuentasAnualesItemReader;

    @Autowired
    private CuentasAnualesItemProcessor cuentasAnualesItemProcessor;

    @Autowired
    private CuentasAnualesItemWriter cuentasAnualesItemWriter;

    @Bean
    public SkipPolicy customTransaccionesSkipPolicy() {
        return new SkipPolicy() {
            @Override
            public boolean shouldSkip(Throwable t, long skipCount) {
                // Log del error para debugging
                log.debug("Evaluando si hacer skip del error en transacciones (count: {}): {} - {}",
                        skipCount + 1, t.getClass().getSimpleName(), t.getMessage());

                // Skip para errores de parsing de fechas
                if (t instanceof java.time.format.DateTimeParseException) {
                    log.debug("Skipping DateTimeParseException en transacciones (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para errores de formato numérico
                if (t instanceof NumberFormatException) {
                    log.debug("Skipping NumberFormatException en transacciones (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para IllegalArgumentException
                if (t instanceof IllegalArgumentException) {
                    log.debug("Skipping IllegalArgumentException en transacciones (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para RuntimeException con mensajes específicos de procesamiento
                if (t instanceof RuntimeException) {
                    String message = t.getMessage();
                    if (message != null && (message.contains("Error al mapear la línea") ||
                            message.contains("Fecha inválida") ||
                            message.contains("Monto inválido") ||
                            message.contains("ID inválido") ||
                            message.contains("Tipo inválido") ||
                            message.contains("Error de procesamiento") ||
                            message.contains("Error de mapeo general") ||
                            message.contains("Formato de") ||
                            message.contains("vacío") ||
                            message.contains("nulo"))) {
                        log.debug("Skipping custom RuntimeException en transacciones (count: {}): {}", skipCount + 1,
                                message);
                        return true;
                    }
                }

                // Skip para errores de validación de datos
                if (t instanceof org.springframework.batch.item.validator.ValidationException) {
                    log.debug("Skipping ValidationException en transacciones (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para errores de parsing general
                if (t instanceof org.springframework.batch.item.file.transform.IncorrectTokenCountException) {
                    log.debug("Skipping IncorrectTokenCountException en transacciones (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // No skip para errores de base de datos o errores críticos del sistema
                if (t instanceof org.springframework.dao.DataAccessException) {
                    log.error("ERROR CRÍTICO DE BASE DE DATOS EN TRANSACCIONES - NO SE PUEDE SKIP (count: {}): {}",
                            skipCount + 1, t.getMessage());
                    return false;
                }

                // No skip para otros tipos de errores críticos
                log.warn("Error no clasificado para skip en transacciones (count: {}): {} - {}",
                        skipCount + 1, t.getClass().getSimpleName(), t.getMessage());
                return false;
            }
        };
    }

    @Bean
    public SkipPolicy customInteresesSkipPolicy() {
        return new SkipPolicy() {
            @Override
            public boolean shouldSkip(Throwable t, long skipCount) {
                // Log del error para debugging
                log.debug("Evaluando si hacer skip del error en intereses (count: {}): {} - {}",
                        skipCount + 1, t.getClass().getSimpleName(), t.getMessage());

                // Skip para errores de formato numérico
                if (t instanceof NumberFormatException) {
                    log.debug("Skipping NumberFormatException en intereses (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para IllegalArgumentException
                if (t instanceof IllegalArgumentException) {
                    log.debug("Skipping IllegalArgumentException en intereses (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para RuntimeException con mensajes específicos de procesamiento
                if (t instanceof RuntimeException) {
                    String message = t.getMessage();
                    if (message != null && (message.contains("Error al mapear la línea") ||
                            message.contains("Saldo inválido") ||
                            message.contains("Edad inválida") ||
                            message.contains("cuenta_id inválido") ||
                            message.contains("Nombre inválido") ||
                            message.contains("Tipo inválido") ||
                            message.contains("Error de procesamiento") ||
                            message.contains("Error de mapeo general") ||
                            message.contains("Formato de") ||
                            message.contains("vacío") ||
                            message.contains("nulo"))) {
                        log.debug("Skipping custom RuntimeException en intereses (count: {}): {}", skipCount + 1,
                                message);
                        return true;
                    }
                }

                // Skip para errores de validación de datos
                if (t instanceof org.springframework.batch.item.validator.ValidationException) {
                    log.debug("Skipping ValidationException en intereses (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para errores de parsing general
                if (t instanceof org.springframework.batch.item.file.transform.IncorrectTokenCountException) {
                    log.debug("Skipping IncorrectTokenCountException en intereses (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // No skip para errores de base de datos o errores críticos del sistema
                if (t instanceof org.springframework.dao.DataAccessException) {
                    log.error("ERROR CRÍTICO DE BASE DE DATOS EN INTERESES - NO SE PUEDE SKIP (count: {}): {}",
                            skipCount + 1, t.getMessage());
                    return false;
                }

                // No skip para otros tipos de errores críticos
                log.warn("Error no clasificado para skip en intereses (count: {}): {} - {}",
                        skipCount + 1, t.getClass().getSimpleName(), t.getMessage());
                return false;
            }
        };
    }

    @Bean
    public SkipPolicy customCuentasAnualesSkipPolicy() {
        return new SkipPolicy() {
            @Override
            public boolean shouldSkip(Throwable t, long skipCount) {
                // Log del error para debugging
                log.debug("Evaluando si hacer skip del error en cuentas anuales (count: {}): {} - {}",
                        skipCount + 1, t.getClass().getSimpleName(), t.getMessage());

                // Skip para errores de parsing de fechas
                if (t instanceof java.time.format.DateTimeParseException) {
                    log.debug("Skipping DateTimeParseException en cuentas anuales (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para errores de formato numérico
                if (t instanceof NumberFormatException) {
                    log.debug("Skipping NumberFormatException en cuentas anuales (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para IllegalArgumentException
                if (t instanceof IllegalArgumentException) {
                    log.debug("Skipping IllegalArgumentException en cuentas anuales (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para RuntimeException con mensajes específicos de procesamiento
                if (t instanceof RuntimeException) {
                    String message = t.getMessage();
                    if (message != null && (message.contains("Error al mapear la línea") ||
                            message.contains("Fecha inválida") ||
                            message.contains("Monto inválido") ||
                            message.contains("cuenta_id inválido") ||
                            message.contains("Transacción inválida") ||
                            message.contains("Descripción inválida") ||
                            message.contains("Error de procesamiento") ||
                            message.contains("Error de mapeo general") ||
                            message.contains("Formato de") ||
                            message.contains("vacío") ||
                            message.contains("nulo"))) {
                        log.debug("Skipping custom RuntimeException en cuentas anuales (count: {}): {}", skipCount + 1,
                                message);
                        return true;
                    }
                }

                // Skip para errores de validación de datos
                if (t instanceof org.springframework.batch.item.validator.ValidationException) {
                    log.debug("Skipping ValidationException en cuentas anuales (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // Skip para errores de parsing general
                if (t instanceof org.springframework.batch.item.file.transform.IncorrectTokenCountException) {
                    log.debug("Skipping IncorrectTokenCountException en cuentas anuales (count: {}): {}", skipCount + 1,
                            t.getMessage());
                    return true;
                }

                // No skip para errores de base de datos o errores críticos del sistema
                if (t instanceof org.springframework.dao.DataAccessException) {
                    log.error("ERROR CRÍTICO DE BASE DE DATOS EN CUENTAS ANUALES - NO SE PUEDE SKIP (count: {}): {}",
                            skipCount + 1, t.getMessage());
                    return false;
                }

                // No skip para otros tipos de errores críticos
                log.warn("Error no clasificado para skip en cuentas anuales (count: {}): {} - {}",
                        skipCount + 1, t.getClass().getSimpleName(), t.getMessage());
                return false;
            }
        };
    }

    @Bean
    public Step readTransaccionesFile(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("readTransaccionesFile", jobRepository)
                .<Transacciones, Transacciones>chunk(50, transactionManager)
                .reader(transaccionesItemReader)
                .processor(transaccionesItemProcessor)
                .writer(transaccionesItemWriter)
                .faultTolerant()
                .skipPolicy(customTransaccionesSkipPolicy())
                .skipLimit(1000)
                .retryLimit(3)
                .retry(Exception.class)
                .allowStartIfComplete(true)
                .startLimit(3)
                .build();
    }

    @Bean
    public Step readInteresesFile(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("readInteresesFile", jobRepository)
                .<Intereses, Intereses>chunk(50, transactionManager)
                .reader(interesesItemReader)
                .processor(interesesItemProcessor)
                .writer(interesesItemWriter)
                .faultTolerant()
                .skipPolicy(customInteresesSkipPolicy())
                .skipLimit(1000)
                .retryLimit(3)
                .retry(Exception.class)
                .allowStartIfComplete(true)
                .startLimit(3)
                .build();
    }

    @Bean
    public Step readCuentasAnualesFile(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("readCuentasAnualesFile", jobRepository)
                .<CuentasAnuales, CuentasAnuales>chunk(50, transactionManager)
                .reader(cuentasAnualesItemReader)
                .processor(cuentasAnualesItemProcessor)
                .writer(cuentasAnualesItemWriter)
                .faultTolerant()
                .skipPolicy(customCuentasAnualesSkipPolicy())
                .skipLimit(1000)
                .retryLimit(3)
                .retry(Exception.class)
                .allowStartIfComplete(true)
                .startLimit(3)
                .build();
    }

    @Bean(name = "job")
    public Job transaccionesJob(JobRepository jobRepository, Step readTransaccionesFile) {
        return new JobBuilder("transaccionesProcessingJob", jobRepository)
                .start(readTransaccionesFile)
                .build();
    }

    @Bean(name = "interesesJob")
    public Job interesesJob(JobRepository jobRepository, Step readInteresesFile) {
        return new JobBuilder("interesesProcessingJob", jobRepository)
                .start(readInteresesFile)
                .build();
    }

    @Bean(name = "cuentasAnualesJob")
    public Job cuentasAnualesJob(JobRepository jobRepository, Step readCuentasAnualesFile) {
        return new JobBuilder("cuentasAnualesProcessingJob", jobRepository)
                .start(readCuentasAnualesFile)
                .build();
    }
}