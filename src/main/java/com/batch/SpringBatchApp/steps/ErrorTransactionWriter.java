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

import com.batch.SpringBatchApp.entities.Transacciones;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ErrorTransactionWriter {

    private static final String ERROR_FILE_NAME = "transacciones_errores.csv";
    private static final String ERROR_DIRECTORY = "error-files";
    private final AtomicBoolean headerWritten = new AtomicBoolean(false);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final Path errorFilePath;

    public ErrorTransactionWriter() {
        try {
            // Crear directorio si no existe
            Path errorDir = Paths.get(ERROR_DIRECTORY);
            if (!Files.exists(errorDir)) {
                Files.createDirectories(errorDir);
                log.info("Directorio de errores creado: {}", errorDir.toAbsolutePath());
            }

            // Crear archivo con timestamp para evitar conflictos
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String fileName = timestamp + "_" + ERROR_FILE_NAME;
            this.errorFilePath = errorDir.resolve(fileName);

            log.info("Archivo de errores configurado en: {}", errorFilePath.toAbsolutePath());

        } catch (IOException e) {
            log.error("Error crítico al crear directorio de archivos de error", e);
            throw new RuntimeException("Error al crear directorio de archivos de error", e);
        }
    }

    // Escribe una transacción con error al archivo CSV
    public synchronized void writeErrorTransaction(Transacciones transaccion, String motivo, String valorOriginal) {
        try {
            // Escribir header si es la primera vez
            if (!headerWritten.getAndSet(true)) {
                writeHeader();
            }

            // Preparar datos para escribir
            String line = buildErrorLine(transaccion, motivo, valorOriginal);

            // Escribir línea al archivo
            Files.write(errorFilePath, (line + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            errorCount.incrementAndGet();

            log.debug("Error registrado en archivo: ID={}, Motivo={}, Valor={}, Total errores={}",
                    transaccion.getId(), motivo, valorOriginal, errorCount.get());

        } catch (IOException e) {
            log.error("Error crítico al escribir transacción con error al archivo: {}", e.getMessage());
            // NO lanzar excepción aquí para evitar detener el procesamiento
            log.error("ADVERTENCIA: No se pudo registrar el error en archivo para transacción ID: {}",
                    transaccion.getId());
        }
    }

    // Escribe una línea de error cuando no se puede crear la transacción
    public synchronized void writeErrorLine(String id, String fecha, String monto, String tipo, String motivo) {
        try {
            // Escribir header si es la primera vez
            if (!headerWritten.getAndSet(true)) {
                writeHeader();
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String line = String.format("%s,%s,%s,%s,%s,%s",
                    escapeCsv(id != null ? id : "N/A"),
                    escapeCsv(fecha != null ? fecha : "N/A"),
                    escapeCsv(monto != null ? monto : "N/A"),
                    escapeCsv(tipo != null ? tipo : "N/A"),
                    escapeCsv(motivo),
                    escapeCsv(timestamp));

            Files.write(errorFilePath, (line + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            errorCount.incrementAndGet();

            log.debug("Línea con error registrada: ID={}, Motivo={}, Total errores={}",
                    id, motivo, errorCount.get());

        } catch (IOException e) {
            log.error("Error crítico al escribir línea con error al archivo: {}", e.getMessage());
            // NO lanzar excepción aquí para evitar detener el procesamiento
            log.error("ADVERTENCIA: No se pudo registrar el error en archivo para ID: {}", id);
        }
    }

    private void writeHeader() throws IOException {
        String header = "id,fecha_original,monto_original,tipo_original,motivo_error,timestamp_error";
        Files.write(errorFilePath, (header + System.lineSeparator()).getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        log.info("Header del archivo de errores escrito correctamente");
    }

    private String buildErrorLine(Transacciones transaccion, String motivo, String valorOriginal) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        return String.format("%s,%s,%s,%s,%s,%s",
                escapeCsv(transaccion.getId() != null ? transaccion.getId().toString() : "N/A"),
                escapeCsv(transaccion.getFecha() != null ? transaccion.getFecha().toString() : valorOriginal),
                escapeCsv(transaccion.getMonto() != null ? transaccion.getMonto().toString() : valorOriginal),
                escapeCsv(transaccion.getTipo() != null ? transaccion.getTipo() : valorOriginal),
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

    // Obtiene el número de transacciones con error escritas
    public long getErrorCount() {
        return errorCount.get();
    }

    // Resetea el contador de errores
    public void resetErrorCount() {
        errorCount.set(0);
        headerWritten.set(false);
        log.info("Contadores de ErrorTransactionWriter reseteados");
    }

    // Verifica si el archivo de errores existe y tiene contenido
    public boolean hasErrors() {
        return errorCount.get() > 0;
    }
}