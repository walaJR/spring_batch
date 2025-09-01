package com.batch.SpringBatchApp;

import java.util.Map;

import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.batch.SpringBatchApp.config.JobSelector;
import com.batch.SpringBatchApp.config.ProcessorConfig;
import com.batch.SpringBatchApp.steps.ErrorInteresesWriter;
import com.batch.SpringBatchApp.steps.ErrorTransactionWriter;
import com.batch.SpringBatchApp.steps.ErrorCuentasAnualesWriter;
import com.batch.SpringBatchApp.steps.InteresesItemProcessor;
import com.batch.SpringBatchApp.steps.TransaccionesItemProcessor;
import com.batch.SpringBatchApp.steps.CuentasAnualesItemProcessor;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class SpringBatchAppApplication {

	@Autowired
	private JobSelector jobSelector;

	@Autowired
	private ProcessorConfig processorConfig;

	@Autowired
	private TransaccionesItemProcessor transaccionesProcessor;

	@Autowired
	private InteresesItemProcessor interesesProcessor;

	@Autowired
	private CuentasAnualesItemProcessor cuentasAnualesProcessor;

	@Autowired
	private ErrorTransactionWriter errorTransactionWriter;

	@Autowired
	private ErrorInteresesWriter errorInteresesWriter;

	@Autowired
	private ErrorCuentasAnualesWriter errorCuentasAnualesWriter;

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchAppApplication.class, args);
	}

	@Bean
	CommandLineRunner init() {
		return args -> {
			try {
				log.info("=== INICIANDO APLICACION SPRING BATCH MULTI-ENTIDAD ===");
				logApplicationConfiguration();

				// Validar configuración de jobs antes de ejecutar
				if (!jobSelector.validateJobConfiguration()) {
					log.error("Configuración de jobs inválida. Terminando aplicación.");
					return;
				}

				// Verificar archivos disponibles
				Map<String, Boolean> fileAvailability = jobSelector.getFileAvailability();
				if (!jobSelector.hasAvailableFiles()) {
					log.error("No se encontraron archivos CSV para procesar");
					log.error("Archivos esperados: transacciones.csv, intereses.csv, cuentas_anuales.csv");
					log.error("Ubicación: src/main/resources/");
					return;
				}

				// Resetear contadores antes de la ejecución
				resetProcessorCounters();

				// Ejecutar todos los jobs disponibles
				log.info("=== INICIANDO PROCESAMIENTO DE ARCHIVOS DISPONIBLES ===");
				Map<String, JobExecution> executionResults = jobSelector.executeAvailableJobs();

				// Generar resumen final
				generateFinalSummary(executionResults, fileAvailability);

			} catch (Exception e) {
				log.error("=== ERROR CRITICO EN LA APLICACION ===");
				log.error("Error ejecutando la aplicación Spring Batch: {}", e.getMessage());
				log.error("Stack trace completo:", e);
				throw e;
			}
		};
	}

	// Registra la configuración de la aplicación al inicio
	private void logApplicationConfiguration() {
		log.info("=== CONFIGURACION DE LA APLICACION ===");
		log.info("Tamaño de chunk: {}", processorConfig.getTamanoChunk());
		log.info("Límite de errores: {}", processorConfig.getLimiteErrores());
		log.info("Número de reintentos: {}", processorConfig.getNumeroReintentos());
		log.info("Monto máximo permitido: ${}", processorConfig.getMontoMaximo());
		log.info("Convertir negativos a positivos: {}", processorConfig.isConvertirNegativos());
		log.info("Directorio de archivos de error: {}", processorConfig.getDirectorioErrores());
		log.info("Año mínimo para fechas: {}", processorConfig.getAnioMinimo());
		log.info("Validar fechas futuras: {}", processorConfig.isValidarFechasFuturas());
		log.info("Edad mínima permitida: {}", processorConfig.getEdadMinima());
		log.info("Edad máxima permitida: {}", processorConfig.getEdadMaxima());

		log.info("=== CONFIGURACION DE VALIDACIONES ===");
		log.info("TRANSACCIONES - Tipos válidos: DEBITO, CREDITO");
		log.info("TRANSACCIONES - Omitir montos cero: {}", processorConfig.isOmitirMontosCero());
		log.info("INTERESES - Tipos válidos: AHORRO, PRESTAMO, HIPOTECA, CREDITO");
		log.info("INTERESES - Omitir saldos cero: {}", processorConfig.isOmitirSaldosCero());
		log.info("INTERESES - Capitalizar nombres: {}", processorConfig.isCapitalizarNombres());
		log.info(
				"CUENTAS ANUALES - Tipos válidos: DEBITO, CREDITO, DEPOSITO, RETIRO, TRANSFERENCIA, INTERES, COMISION, AJUSTE, CARGO, ABONO");
		log.info("CUENTAS ANUALES - Permitir montos cero: true (ajustes pueden tener $0)");
	}

	// Resetea los contadores de todos los procesadores
	private void resetProcessorCounters() {
		log.info("Reseteando contadores de procesadores...");
		transaccionesProcessor.resetCounters();
		interesesProcessor.resetCounters();
		cuentasAnualesProcessor.resetCounters();
		errorTransactionWriter.resetErrorCount();
		errorInteresesWriter.resetErrorCount();
		errorCuentasAnualesWriter.resetErrorCount();
		log.info("✓ Contadores reseteados exitosamente");
	}

	// Genera un resumen final detallado de la ejecución
	private void generateFinalSummary(Map<String, JobExecution> executionResults,
			Map<String, Boolean> fileAvailability) {

		log.info("=== RESUMEN FINAL DE LA EJECUCION ===");

		// Resumen de archivos procesados
		log.info("ARCHIVOS PROCESADOS:");
		fileAvailability.forEach((jobType, available) -> {
			boolean executed = executionResults.containsKey(jobType);
			String status = !available ? "NO ENCONTRADO" : executed ? "PROCESADO" : "DISPONIBLE PERO NO EJECUTADO";
			log.info("  - {}.csv: {}", jobType.toUpperCase(), status);
		});

		// Resumen de estadísticas de procesamiento
		log.info("ESTADISTICAS DE PROCESAMIENTO:");

		if (executionResults.containsKey("transacciones")) {
			log.info("TRANSACCIONES:");
			transaccionesProcessor.logProcessingStats();
			log.info("  - Archivo de errores: {}", errorTransactionWriter.getErrorFilePath());
			log.info("  - Registros con errores: {}", errorTransactionWriter.getErrorCount());
		}

		if (executionResults.containsKey("intereses")) {
			log.info("INTERESES:");
			interesesProcessor.logProcessingStats();
			log.info("  - Archivo de errores: {}", errorInteresesWriter.getErrorFilePath());
			log.info("  - Registros con errores: {}", errorInteresesWriter.getErrorCount());
		}

		if (executionResults.containsKey("cuentas_anuales")) {
			log.info("CUENTAS ANUALES:");
			cuentasAnualesProcessor.logProcessingStats();
			log.info("  - Archivo de errores: {}", errorCuentasAnualesWriter.getErrorFilePath());
			log.info("  - Registros con errores: {}", errorCuentasAnualesWriter.getErrorCount());
		}

		// Resumen de ejecuciones de jobs
		log.info("RESULTADO DE JOBS:");
		if (executionResults.isEmpty()) {
			log.warn("  - No se ejecutaron jobs (archivos no encontrados)");
		} else {
			executionResults.forEach((jobType, execution) -> {
				String exitCode = execution.getExitStatus().getExitCode();
				String status = execution.getStatus().toString();
				log.info("  - Job {}: {} (Exit Code: {})",
						jobType.toUpperCase(), status, exitCode);

				// Mostrar estadísticas detalladas de steps
				execution.getStepExecutions().forEach(stepExecution -> {
					log.info("    - Step: {}", stepExecution.getStepName());
					log.info("      Read: {}, Write: {}, Skip: {}, Filter: {}",
							stepExecution.getReadCount(),
							stepExecution.getWriteCount(),
							stepExecution.getSkipCount(),
							stepExecution.getFilterCount());
				});
			});
		}

		// Resumen de archivos de error generados
		log.info("ARCHIVOS DE ERROR GENERADOS:");
		boolean hasErrors = false;

		if (errorTransactionWriter.getErrorCount() > 0) {
			log.info("  - Transacciones con errores: {}", errorTransactionWriter.getErrorFilePath());
			hasErrors = true;
		}

		if (errorInteresesWriter.getErrorCount() > 0) {
			log.info("  - Intereses con errores: {}", errorInteresesWriter.getErrorFilePath());
			hasErrors = true;
		}

		if (errorCuentasAnualesWriter.getErrorCount() > 0) {
			log.info("  - Cuentas anuales con errores: {}", errorCuentasAnualesWriter.getErrorFilePath());
			hasErrors = true;
		}

		if (!hasErrors) {
			log.info("  - No se generaron archivos de error");
		}

		// Mensaje final
		if (executionResults.isEmpty()) {
			log.warn("=== PROCESAMIENTO COMPLETADO SIN ARCHIVOS PROCESADOS ===");
			log.warn("Verifique que los archivos CSV estén en el directorio src/main/resources/");
		} else {
			boolean allSuccessful = executionResults.values().stream()
					.allMatch(execution -> "COMPLETED".equals(execution.getStatus().toString()));

			if (allSuccessful) {
				log.info("=== PROCESAMIENTO COMPLETADO EXITOSAMENTE ===");
				log.info("Archivos procesados: {}/{}", executionResults.size(), fileAvailability.size());
			} else {
				log.warn("=== PROCESAMIENTO COMPLETADO CON ERRORES ===");
				log.warn("Revise los logs anteriores para detalles de los errores");
			}
		}
	}

	// Maneja la finalización de la aplicación con cleanup
	private void performCleanup() {
		log.info("Realizando limpieza final...");
		// Aquí se pueden agregar tareas de limpieza adicionales si es necesario
		log.info("Limpieza completada");
	}
}