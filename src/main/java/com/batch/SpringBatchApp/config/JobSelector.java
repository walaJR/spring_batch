package com.batch.SpringBatchApp.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class JobSelector {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("job")
    private Job transaccionesJob;

    @Autowired
    @Qualifier("interesesJob")
    private Job interesesJob;

    @Autowired
    @Qualifier("cuentasAnualesJob")
    private Job cuentasAnualesJob;

    private final Map<String, String> jobFileMapping = new HashMap<>();

    public JobSelector() {
        // Mapear cada job con su archivo correspondiente
        jobFileMapping.put("transacciones", "transacciones.csv");
        jobFileMapping.put("intereses", "intereses.csv");
        jobFileMapping.put("cuentas_anuales", "cuentas_anuales.csv");
    }

    // Ejecuta el job de transacciones si el archivo existe
    public JobExecution executeTransaccionesJob() throws Exception {
        log.info("=== INICIANDO EJECUCIÓN DEL JOB DE TRANSACCIONES ===");

        if (!checkFileExists("transacciones.csv")) {
            log.warn("Archivo transacciones.csv no encontrado, saltando ejecución");
            return null;
        }

        JobParameters jobParameters = createJobParameters("transacciones");

        log.info("Ejecutando job de transacciones con parámetros: {}", jobParameters.getParameters());
        JobExecution execution = jobLauncher.run(transaccionesJob, jobParameters);

        log.info("Job de transacciones completado con estado: {}", execution.getStatus());
        return execution;
    }

    // Ejecuta el job de intereses si el archivo existe
    public JobExecution executeInteresesJob() throws Exception {
        log.info("=== INICIANDO EJECUCIÓN DEL JOB DE INTERESES ===");

        if (!checkFileExists("intereses.csv")) {
            log.warn("Archivo intereses.csv no encontrado, saltando ejecución");
            return null;
        }

        JobParameters jobParameters = createJobParameters("intereses");

        log.info("Ejecutando job de intereses con parámetros: {}", jobParameters.getParameters());
        JobExecution execution = jobLauncher.run(interesesJob, jobParameters);

        log.info("Job de intereses completado con estado: {}", execution.getStatus());
        return execution;
    }

    // Ejecuta el job de cuentas anuales si el archivo existe
    public JobExecution executeCuentasAnualesJob() throws Exception {
        log.info("=== INICIANDO EJECUCIÓN DEL JOB DE CUENTAS ANUALES ===");

        if (!checkFileExists("cuentas_anuales.csv")) {
            log.warn("Archivo cuentas_anuales.csv no encontrado, saltando ejecución");
            return null;
        }

        JobParameters jobParameters = createJobParameters("cuentas_anuales");

        log.info("Ejecutando job de cuentas anuales con parámetros: {}", jobParameters.getParameters());
        JobExecution execution = jobLauncher.run(cuentasAnualesJob, jobParameters);

        log.info("Job de cuentas anuales completado con estado: {}", execution.getStatus());
        return execution;
    }

    // Ejecuta todos los jobs disponibles según los archivos presentes
    public Map<String, JobExecution> executeAvailableJobs() {
        Map<String, JobExecution> results = new HashMap<>();

        log.info("=== INICIANDO EJECUCIÓN DE JOBS DISPONIBLES ===");

        try {
            // Ejecutar job de transacciones si está disponible
            JobExecution transaccionesResult = executeTransaccionesJob();
            if (transaccionesResult != null) {
                results.put("transacciones", transaccionesResult);
                log.info("✓ Job de transacciones ejecutado exitosamente");
            } else {
                log.info("◯ Job de transacciones saltado (archivo no encontrado)");
            }

        } catch (Exception e) {
            log.error("✗ Error al ejecutar job de transacciones: {}", e.getMessage(), e);
            // Continuar con otros jobs aunque uno falle
        }

        try {
            // Ejecutar job de intereses si está disponible
            JobExecution interesesResult = executeInteresesJob();
            if (interesesResult != null) {
                results.put("intereses", interesesResult);
                log.info("✓ Job de intereses ejecutado exitosamente");
            } else {
                log.info("◯ Job de intereses saltado (archivo no encontrado)");
            }

        } catch (Exception e) {
            log.error("✗ Error al ejecutar job de intereses: {}", e.getMessage(), e);
        }

        try {
            // Ejecutar job de cuentas anuales si está disponible
            JobExecution cuentasAnualesResult = executeCuentasAnualesJob();
            if (cuentasAnualesResult != null) {
                results.put("cuentas_anuales", cuentasAnualesResult);
                log.info("✓ Job de cuentas anuales ejecutado exitosamente");
            } else {
                log.info("◯ Job de cuentas anuales saltado (archivo no encontrado)");
            }

        } catch (Exception e) {
            log.error("✗ Error al ejecutar job de cuentas anuales: {}", e.getMessage(), e);
        }

        log.info("=== RESUMEN DE EJECUCIÓN DE JOBS ===");
        log.info("Jobs ejecutados exitosamente: {}", results.size());
        results.forEach((jobName, execution) -> log.info("  - {}: {} ({})", jobName, execution.getStatus(),
                execution.getExitStatus().getExitCode()));

        return results;
    }

    // Verifica si un archivo específico existe en el classpath
    public boolean checkFileExists(String fileName) {
        try {
            ClassPathResource resource = new ClassPathResource(fileName);
            boolean exists = resource.exists();
            log.debug("Verificación de archivo {}: {}", fileName, exists ? "ENCONTRADO" : "NO ENCONTRADO");
            return exists;
        } catch (Exception e) {
            log.warn("Error al verificar archivo {}: {}", fileName, e.getMessage());
            return false;
        }
    }

    // Obtiene información sobre la disponibilidad de archivos para cada job
    public Map<String, Boolean> getFileAvailability() {
        Map<String, Boolean> availability = new HashMap<>();

        jobFileMapping.forEach((jobName, fileName) -> {
            boolean available = checkFileExists(fileName);
            availability.put(jobName, available);
            log.info("Archivo para job {}: {} - {}", jobName, fileName,
                    available ? "DISPONIBLE" : "NO DISPONIBLE");
        });

        return availability;
    }

    // Crea parámetros específicos para un job
    private JobParameters createJobParameters(String jobType) {
        return new JobParametersBuilder()
                .addString("jobType", jobType)
                .addString("fileName", jobFileMapping.get(jobType))
                .addLong("timestamp", System.currentTimeMillis())
                .addString("executionId", java.util.UUID.randomUUID().toString())
                .toJobParameters();
    }

    // Verifica si hay al menos un archivo disponible para procesamiento
    public boolean hasAvailableFiles() {
        return jobFileMapping.values().stream()
                .anyMatch(this::checkFileExists);
    }

    // Obtiene el número total de archivos disponibles
    public int getAvailableFileCount() {
        return (int) jobFileMapping.values().stream()
                .mapToLong(fileName -> checkFileExists(fileName) ? 1 : 0)
                .sum();
    }

    // Valida que todos los jobs necesarios estén configurados correctamente.
    // Retorna true si todos los jobs están disponibles

    public boolean validateJobConfiguration() {
        boolean valid = true;

        if (transaccionesJob == null) {
            log.error("Job de transacciones no está configurado correctamente");
            valid = false;
        }

        if (interesesJob == null) {
            log.error("Job de intereses no está configurado correctamente");
            valid = false;
        }

        if (cuentasAnualesJob == null) {
            log.error("Job de cuentas anuales no está configurado correctamente");
            valid = false;
        }

        if (jobLauncher == null) {
            log.error("JobLauncher no está configurado correctamente");
            valid = false;
        }

        if (valid) {
            log.info("✓ Configuración de jobs validada correctamente");
        } else {
            log.error("✗ Errores en la configuración de jobs detectados");
        }

        return valid;
    }
}