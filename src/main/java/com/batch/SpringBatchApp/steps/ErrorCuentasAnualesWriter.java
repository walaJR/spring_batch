package com.batch.SpringBatchApp.steps;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.entities.CuentasAnuales;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ErrorCuentasAnualesWriter {

    private static final String ERROR_FILE_NAME = "cuentas_anuales_errores.csv";
    private static final String ERROR_DIRECTORY = "error-files";
    private final AtomicBoolean headerWritten = new AtomicBoolean(false);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final Path errorFilePath;

    public ErrorCuentasAnualesWriter() {
        try {
            // Crear directorio si no existe
            Path errorDir = Paths.get(ERROR_DIRECTORY);
            if (!Files.exists(errorDir)) {
                Files.createDirectories(errorDir);
                log.info("Directorio de errores para cuentas anuales creado: {}", errorDir.toAbsolutePath());
            }

            // Crear archivo con timestamp para evitar conflictos
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String fileName = timestamp + "_" + ERROR_FILE_NAME;
            this.errorFilePath = errorDir.resolve(fileName);

            log.info("Archivo de errores de cuentas anuales configurado en: {}", errorFilePath.toAbsolutePath());

        } catch (IOException e) {
            log.error("Error crítico al crear directorio de archivos de error para cuentas anuales", e);
            throw new RuntimeException("Error al crear directorio de archivos de error para cuentas anuales", e);
        }
    }

    // Escribe un registro de cuenta anual con error al archivo CSV
    public synchronized void writeErrorCuentaAnual(CuentasAnuales cuentaAnual, String motivo, String valorOriginal) {
        try {
            // Escribir header si es la primera vez
            if (!headerWritten.getAndSet(true)) {
                writeHeader();
            }

            // Preparar datos para escribir
            String line = buildErrorLine(cuentaAnual, motivo, valorOriginal);

            // Escribir línea al archivo
            Files.write(errorFilePath, (line + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            errorCount.incrementAndGet();

            log.debug("Error de cuenta anual registrado en archivo: ID={}, Motivo={}, Valor={}, Total errores={}",
                    cuentaAnual.getCuenta_id(), motivo, valorOriginal, errorCount.get());

        } catch (IOException e) {
            log.error("Error crítico al escribir registro de cuenta anual con error al archivo: {}", e.getMessage());
            log.error("ADVERTENCIA: No se pudo registrar el error en archivo para cuenta anual ID: {}",
                    cuentaAnual.getCuenta_id());
        }
    }

    // Escribe una línea de error cuando no se puede crear el registro de cuenta
    // anual
    public synchronized void writeErrorLine(String cuenta_id, String fecha, String transaccion, String monto,
            String descripcion, String motivo) {
        try {
            // Escribir header si es la primera vez
            if (!headerWritten.getAndSet(true)) {
                writeHeader();
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String line = String.format("%s,%s,%s,%s,%s,%s,%s",
                    escapeCsv(cuenta_id != null ? cuenta_id : "N/A"),
                    escapeCsv(fecha != null ? fecha : "N/A"),
                    escapeCsv(transaccion != null ? transaccion : "N/A"),
                    escapeCsv(monto != null ? monto : "N/A"),
                    escapeCsv(descripcion != null ? descripcion : "N/A"),
                    escapeCsv(motivo),
                    escapeCsv(timestamp));

            Files.write(errorFilePath, (line + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            errorCount.incrementAndGet();

            log.debug("Línea de cuenta anual con error registrada: ID={}, Motivo={}, Total errores={}",
                    cuenta_id, motivo, errorCount.get());

        } catch (IOException e) {
            log.error("Error crítico al escribir línea de cuenta anual con error al archivo: {}", e.getMessage());
            log.error("ADVERTENCIA: No se pudo registrar el error en archivo para cuenta ID: {}", cuenta_id);
        }
    }

    private void writeHeader() throws IOException {
        String header = "cuenta_id,fecha_original,transaccion_original,monto_original,descripcion_original,motivo_error,timestamp_error";
        Files.write(errorFilePath, (header + System.lineSeparator()).getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        log.info("Header del archivo de errores de cuentas anuales escrito correctamente");
    }

    private String buildErrorLine(CuentasAnuales cuentaAnual, String motivo, String valorOriginal) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        return String.format("%s,%s,%s,%s,%s,%s,%s",
                escapeCsv(cuentaAnual.getCuenta_id() != null ? cuentaAnual.getCuenta_id().toString() : "N/A"),
                escapeCsv(cuentaAnual.getFecha() != null ? cuentaAnual.getFecha().toString() : valorOriginal),
                escapeCsv(cuentaAnual.getTransaccion() != null ? cuentaAnual.getTransaccion() : valorOriginal),
                escapeCsv(cuentaAnual.getMonto() != null ? cuentaAnual.getMonto().toString() : valorOriginal),
                escapeCsv(cuentaAnual.getDescripcion() != null ? cuentaAnual.getDescripcion() : valorOriginal),
                escapeCsv(motivo),
                escapeCsv(timestamp));
    }

    // Escapa caracteres especiales para CSV
    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }

        // Si contiene coma, comillas o salto de línea, envolver en comillas
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            // Escapar comillas dobles duplicándolas
            value = value.replace("\"", "\"\"");
            return "\"" + value + "\"";
        }

        return value;
    }

    // Obtiene la ruta del archivo de errores
    public String getErrorFilePath() {
        return errorFilePath.toAbsolutePath().toString();
    }

    // Obtiene el número de registros de cuenta anual con error escritos
    public long getErrorCount() {
        return errorCount.get();
    }

    // Resetea el contador de errores
    public void resetErrorCount() {
        errorCount.set(0);
        headerWritten.set(false);
        log.info("Contadores de ErrorCuentasAnualesWriter reseteados");
    }

    // Verifica si el archivo de errores existe y tiene contenido
    public boolean hasErrors() {
        return errorCount.get() > 0;
    }
}