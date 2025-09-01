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

import com.batch.SpringBatchApp.entities.Intereses;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ErrorInteresesWriter {

    private static final String ERROR_FILE_NAME = "intereses_errores.csv";
    private static final String ERROR_DIRECTORY = "error-files";
    private final AtomicBoolean headerWritten = new AtomicBoolean(false);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final Path errorFilePath;

    public ErrorInteresesWriter() {
        try {
            // Crear directorio si no existe
            Path errorDir = Paths.get(ERROR_DIRECTORY);
            if (!Files.exists(errorDir)) {
                Files.createDirectories(errorDir);
                log.info("Directorio de errores para intereses creado: {}", errorDir.toAbsolutePath());
            }

            // Crear archivo con timestamp para evitar conflictos
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String fileName = timestamp + "_" + ERROR_FILE_NAME;
            this.errorFilePath = errorDir.resolve(fileName);

            log.info("Archivo de errores de intereses configurado en: {}", errorFilePath.toAbsolutePath());

        } catch (IOException e) {
            log.error("Error crítico al crear directorio de archivos de error para intereses", e);
            throw new RuntimeException("Error al crear directorio de archivos de error para intereses", e);
        }
    }

    // Escribe un registro de intereses con error al archivo CSV
    public synchronized void writeErrorInteres(Intereses interes, String motivo, String valorOriginal) {
        try {
            // Escribir header si es la primera vez
            if (!headerWritten.getAndSet(true)) {
                writeHeader();
            }

            // Preparar datos para escribir
            String line = buildErrorLine(interes, motivo, valorOriginal);

            // Escribir línea al archivo
            Files.write(errorFilePath, (line + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            errorCount.incrementAndGet();

            log.debug("Error de interés registrado en archivo: ID={}, Motivo={}, Valor={}, Total errores={}",
                    interes.getCuenta_id(), motivo, valorOriginal, errorCount.get());

        } catch (IOException e) {
            log.error("Error crítico al escribir registro de interés con error al archivo: {}", e.getMessage());
            log.error("ADVERTENCIA: No se pudo registrar el error en archivo para interés ID: {}",
                    interes.getCuenta_id());
        }
    }

    // Escribe una línea de error cuando no se puede crear el registro de interés
    public synchronized void writeErrorLine(String cuenta_id, String nombre, String saldo, String edad, String tipo,
            String motivo) {
        try {
            // Escribir header si es la primera vez
            if (!headerWritten.getAndSet(true)) {
                writeHeader();
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String line = String.format("%s,%s,%s,%s,%s,%s,%s",
                    escapeCsv(cuenta_id != null ? cuenta_id : "N/A"),
                    escapeCsv(nombre != null ? nombre : "N/A"),
                    escapeCsv(saldo != null ? saldo : "N/A"),
                    escapeCsv(edad != null ? edad : "N/A"),
                    escapeCsv(tipo != null ? tipo : "N/A"),
                    escapeCsv(motivo),
                    escapeCsv(timestamp));

            Files.write(errorFilePath, (line + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            errorCount.incrementAndGet();

            log.debug("Línea de interés con error registrada: ID={}, Motivo={}, Total errores={}",
                    cuenta_id, motivo, errorCount.get());

        } catch (IOException e) {
            log.error("Error crítico al escribir línea de interés con error al archivo: {}", e.getMessage());
            log.error("ADVERTENCIA: No se pudo registrar el error en archivo para cuenta ID: {}", cuenta_id);
        }
    }

    private void writeHeader() throws IOException {
        String header = "cuenta_id,nombre_original,saldo_original,edad_original,tipo_original,motivo_error,timestamp_error";
        Files.write(errorFilePath, (header + System.lineSeparator()).getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        log.info("Header del archivo de errores de intereses escrito correctamente");
    }

    private String buildErrorLine(Intereses interes, String motivo, String valorOriginal) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        return String.format("%s,%s,%s,%s,%s,%s,%s",
                escapeCsv(interes.getCuenta_id() != null ? interes.getCuenta_id().toString() : "N/A"),
                escapeCsv(interes.getNombre() != null ? interes.getNombre() : valorOriginal),
                escapeCsv(interes.getSaldo() != null ? interes.getSaldo().toString() : valorOriginal),
                escapeCsv(String.valueOf(interes.getEdad())),
                escapeCsv(interes.getTipo() != null ? interes.getTipo() : valorOriginal),
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

    // Obtiene el número de registros de interés con error escritos
    public long getErrorCount() {
        return errorCount.get();
    }

    // Resetea el contador de errores
    public void resetErrorCount() {
        errorCount.set(0);
        headerWritten.set(false);
        log.info("Contadores de ErrorInteresesWriter reseteados");
    }

    // Verifica si el archivo de errores existe y tiene contenido
    public boolean hasErrors() {
        return errorCount.get() > 0;
    }
}