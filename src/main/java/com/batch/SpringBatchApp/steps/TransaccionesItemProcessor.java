package com.batch.SpringBatchApp.steps;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.config.ProcessorConfig;
import com.batch.SpringBatchApp.entities.Transacciones;
import com.batch.SpringBatchApp.utils.DateParser;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TransaccionesItemProcessor implements ItemProcessor<Transacciones, Transacciones> {

    // Tipos de transacción válidos - SOLO debito y credito según requerimientos
    private static final List<String> TIPOS_VALIDOS = Arrays.asList("DEBITO", "CREDITO");

    // Tipos que deben ser enviados al archivo de errores
    private static final List<String> TIPOS_INVALIDOS = Arrays.asList("INVALID", "DESCONOCIDO", "UNKNOWN", "ERROR");

    @Autowired
    private ProcessorConfig processorConfig;

    @Autowired
    private ErrorTransactionWriter errorWriter;

    @Autowired
    private DateParser dateParser;

    // Contadores para estadísticas
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong validCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong skippedCount = new AtomicLong(0);

    @Override
    @Nullable
    public Transacciones process(@NonNull Transacciones item) throws Exception {
        processedCount.incrementAndGet();

        log.debug("Procesando transacción: ID={}, Tipo={}", item.getId(), item.getTipo());

        // Verificar si es un marcador de error del reader
        if (isErrorMarker(item)) {
            log.debug("Marcador de error detectado, saltando: ID={}", item.getId());
            skippedCount.incrementAndGet();
            return null; // Filtrar - no procesar más
        }

        try {
            // Validación de campos obligatorios
            if (!validarCamposObligatorios(item)) {
                errorWriter.writeErrorTransaction(item, "Campos obligatorios faltantes", "N/A");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de fecha (validación adicional del processor)
            if (!validarFecha(item)) {
                errorWriter.writeErrorTransaction(item, "Fecha inválida en processor",
                        item.getFecha() != null ? item.getFecha().toString() : "null");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de monto
            if (!validarMonto(item)) {
                errorWriter.writeErrorTransaction(item, "Monto inválido",
                        item.getMonto() != null ? item.getMonto().toString() : "null");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación crítica de tipo - RECHAZAR tipos inválidos
            String tipoNormalizado = normalizarTipo(item.getTipo());
            if (!TIPOS_VALIDOS.contains(tipoNormalizado)) {
                log.warn("Transacción rechazada por tipo inválido: ID={}, Tipo original='{}', Tipo normalizado='{}'",
                        item.getId(), item.getTipo(), tipoNormalizado);
                errorWriter.writeErrorTransaction(item, "Tipo de transacción no válido", item.getTipo());
                errorCount.incrementAndGet();
                return null;
            }

            // Si llegamos aquí, la transacción es válida - crear versión procesada
            Transacciones transaccionProcesada = crearTransaccionProcesada(item, tipoNormalizado);
            validCount.incrementAndGet();

            log.info("Transacción procesada exitosamente: ID={}, Tipo={}, Monto={}",
                    transaccionProcesada.getId(), transaccionProcesada.getTipo(),
                    transaccionProcesada.getMonto());

            return transaccionProcesada;

        } catch (Exception e) {
            log.error("Error inesperado al procesar transacción ID={}: {}", item.getId(), e.getMessage(), e);
            errorWriter.writeErrorTransaction(item, "Error de procesamiento: " + e.getMessage(), "N/A");
            errorCount.incrementAndGet();
            return null; // Filtrar en lugar de lanzar excepción
        }
    }

    // Verifica si la transacción es un marcador de error del reader
    private boolean isErrorMarker(Transacciones item) {
        return item.getTipo() != null && item.getTipo().startsWith("__ERROR_MARKER__");
    }

    private Transacciones crearTransaccionProcesada(Transacciones original, String tipoNormalizado) {
        Transacciones procesada = new Transacciones();
        procesada.setId(original.getId());
        procesada.setFecha(original.getFecha());
        procesada.setTipo(tipoNormalizado);

        // Aplicar transformaciones al monto
        BigDecimal montoTransformado = transformarMonto(original.getMonto());
        procesada.setMonto(montoTransformado);

        return procesada;
    }

    private boolean validarCamposObligatorios(Transacciones item) {
        if (item.getId() == null || item.getId() <= 0) {
            log.debug("ID inválido: {}", item.getId());
            return false;
        }

        if (item.getFecha() == null) {
            log.debug("Fecha nula para ID: {}", item.getId());
            return false;
        }

        if (item.getMonto() == null) {
            log.debug("Monto nulo para ID: {}", item.getId());
            return false;
        }

        if (item.getTipo() == null || item.getTipo().trim().isEmpty()) {
            log.debug("Tipo nulo o vacío para ID: {}", item.getId());
            return false;
        }

        return true;
    }

    private boolean validarFecha(Transacciones item) {
        LocalDate fecha = item.getFecha();

        // Usar el validador del DateParser para consistencia
        if (!dateParser.isValidDate(fecha)) {
            log.debug("Fecha inválida según DateParser: {} para ID: {}", fecha, item.getId());
            return false;
        }

        // Validaciones adicionales específicas del procesador
        if (processorConfig.isValidarFechasFuturas()) {
            LocalDate ahora = LocalDate.now();
            if (fecha.isAfter(ahora)) {
                log.debug("Fecha futura no permitida: {} para ID: {}", fecha, item.getId());
                return false;
            }
        }

        LocalDate fechaMinima = LocalDate.of(processorConfig.getAnioMinimo(), 1, 1);
        if (fecha.isBefore(fechaMinima)) {
            log.debug("Fecha anterior al año mínimo {}: {} para ID: {}",
                    processorConfig.getAnioMinimo(), fecha, item.getId());
            return false;
        }

        return true;
    }

    private boolean validarMonto(Transacciones item) {
        BigDecimal monto = item.getMonto();

        // Validar que no sea cero si está configurado para omitirlos
        if (processorConfig.isOmitirMontosCero() && monto.compareTo(BigDecimal.ZERO) == 0) {
            log.debug("Monto cero no permitido para ID: {}", item.getId());
            return false;
        }

        // Validar rangos máximos
        BigDecimal montoMaximo = new BigDecimal(processorConfig.getMontoMaximo());
        if (monto.abs().compareTo(montoMaximo) > 0) {
            log.debug("Monto excede el límite permitido: {} para ID: {}", monto, item.getId());
            return false;
        }

        return true;
    }

    private BigDecimal transformarMonto(BigDecimal monto) {
        if (processorConfig.isConvertirNegativos() && monto.compareTo(BigDecimal.ZERO) < 0) {
            log.debug("Convirtiendo monto negativo {} a positivo", monto);
            return monto.abs();
        }
        return monto;
    }

    private String normalizarTipo(String tipo) {
        if (tipo == null) {
            return null;
        }

        String tipoLimpio = tipo.trim().toUpperCase();

        // Verificar si es un tipo explícitamente inválido
        if (TIPOS_INVALIDOS.contains(tipoLimpio)) {
            log.debug("Tipo identificado como inválido: {}", tipo);
            return tipoLimpio; // Retornar para que sea rechazado posteriormente
        }

        // Mapear tipos comunes a los estándar (SOLO debito y credito)
        switch (tipoLimpio) {
            case "DEBITO":
            case "DEBIT":
            case "DB":
            case "RETIRO":
            case "WITHDRAWAL":
            case "EGRESO":
                return "DEBITO";

            case "CREDITO":
            case "CREDIT":
            case "CR":
            case "DEPOSITO":
            case "DEPOSIT":
            case "INGRESO":
                return "CREDITO";

            default:
                log.debug("Tipo no reconocido, será rechazado: {}", tipo);
                return tipoLimpio; // Retornar el tipo sin cambios para que sea rechazado
        }
    }

    // Crea un marcador de error para transacciones que no se pueden procesar
    private Transacciones createErrorMarker(Long id, String errorReason) {
        Transacciones errorMarker = new Transacciones();
        errorMarker.setId(id != null ? id : -1L);
        errorMarker.setTipo("__ERROR_MARKER__:" + errorReason);
        // Los demás campos quedan null para que sea filtrado
        return errorMarker;
    }

    // Obtiene estadísticas del procesamiento
    public void logProcessingStats() {
        log.info("=== ESTADÍSTICAS DE PROCESAMIENTO ===");
        log.info("Total procesados en processor: {}", processedCount.get());
        log.info("Transacciones válidas enviadas al writer: {}", validCount.get());
        log.info("Transacciones con errores filtradas: {}", errorCount.get());
        log.info("Marcadores de error saltados: {}", skippedCount.get());
        log.info("Archivo de errores: {}", errorWriter.getErrorFilePath());
        log.info("Tipos válidos aceptados: {}", TIPOS_VALIDOS);
        log.info("Tipos inválidos rechazados automáticamente: {}", TIPOS_INVALIDOS);
    }

    // Resetea los contadores para una nueva ejecución
    public void resetCounters() {
        processedCount.set(0);
        validCount.set(0);
        errorCount.set(0);
        skippedCount.set(0);
    }
}