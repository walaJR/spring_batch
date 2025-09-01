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
import com.batch.SpringBatchApp.entities.CuentasAnuales;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CuentasAnualesItemProcessor implements ItemProcessor<CuentasAnuales, CuentasAnuales> {

    // Tipos de transacción válidos para cuentas anuales (más amplios que
    // transacciones regulares)
    private static final List<String> TIPOS_VALIDOS = Arrays.asList(
            "DEBITO", "CREDITO", "DEPOSITO", "RETIRO", "TRANSFERENCIA",
            "INTERES", "COMISION", "AJUSTE", "CARGO", "ABONO");

    // Tipos que deben ser enviados al archivo de errores
    private static final List<String> TIPOS_INVALIDOS = Arrays.asList(
            "-1", "INVALID", "DESCONOCIDO", "UNKNOWN", "ERROR", "NULL");

    @Autowired
    private ProcessorConfig processorConfig;

    @Autowired
    private ErrorCuentasAnualesWriter errorWriter;

    // Contadores para estadísticas
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong validCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong skippedCount = new AtomicLong(0);

    @Override
    @Nullable
    public CuentasAnuales process(@NonNull CuentasAnuales item) throws Exception {
        processedCount.incrementAndGet();

        log.debug("Procesando cuenta anual: cuenta_id={}, transaccion={}", item.getCuenta_id(), item.getTransaccion());

        // Verificar si es un marcador de error del reader
        if (isErrorMarker(item)) {
            log.debug("Marcador de error detectado, saltando: cuenta_id={}", item.getCuenta_id());
            skippedCount.incrementAndGet();
            return null; // Filtrar - no procesar más
        }

        try {
            // Validación de campos obligatorios
            if (!validarCamposObligatorios(item)) {
                errorWriter.writeErrorCuentaAnual(item, "Campos obligatorios faltantes", "N/A");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de cuenta_id
            if (!validarCuentaId(item)) {
                errorWriter.writeErrorCuentaAnual(item, "cuenta_id inválido",
                        item.getCuenta_id() != null ? item.getCuenta_id().toString() : "null");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de fecha
            if (!validarFecha(item)) {
                errorWriter.writeErrorCuentaAnual(item, "Fecha inválida",
                        item.getFecha() != null ? item.getFecha().toString() : "null");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de monto
            if (!validarMonto(item)) {
                errorWriter.writeErrorCuentaAnual(item, "Monto inválido",
                        item.getMonto() != null ? item.getMonto().toString() : "null");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de descripción
            if (!validarDescripcion(item)) {
                errorWriter.writeErrorCuentaAnual(item, "Descripción inválida", item.getDescripcion());
                errorCount.incrementAndGet();
                return null;
            }

            // Validación crítica de tipo de transacción - RECHAZAR tipos inválidos
            String transaccionNormalizada = normalizarTipoTransaccion(item.getTransaccion());
            if (!TIPOS_VALIDOS.contains(transaccionNormalizada)) {
                log.warn(
                        "Cuenta anual rechazada por tipo de transacción inválido: cuenta_id={}, Transaccion original='{}', Transaccion normalizada='{}'",
                        item.getCuenta_id(), item.getTransaccion(), transaccionNormalizada);
                errorWriter.writeErrorCuentaAnual(item, "Tipo de transacción no válido", item.getTransaccion());
                errorCount.incrementAndGet();
                return null;
            }

            // Si llegamos aquí, el registro es válido - crear versión procesada
            CuentasAnuales cuentaAnualProcesada = crearCuentaAnualProcesada(item, transaccionNormalizada);
            validCount.incrementAndGet();

            log.info(
                    "Cuenta anual procesada exitosamente: cuenta_id={}, fecha={}, transaccion={}, monto={}, descripcion={}",
                    cuentaAnualProcesada.getCuenta_id(), cuentaAnualProcesada.getFecha(),
                    cuentaAnualProcesada.getTransaccion(), cuentaAnualProcesada.getMonto(),
                    cuentaAnualProcesada.getDescripcion().substring(0,
                            Math.min(50, cuentaAnualProcesada.getDescripcion().length())) + "...");

            return cuentaAnualProcesada;

        } catch (Exception e) {
            log.error("Error inesperado al procesar cuenta anual cuenta_id={}: {}", item.getCuenta_id(), e.getMessage(),
                    e);
            errorWriter.writeErrorCuentaAnual(item, "Error de procesamiento: " + e.getMessage(), "N/A");
            errorCount.incrementAndGet();
            return null; // Filtrar en lugar de lanzar excepción
        }
    }

    // Verifica si el registro es un marcador de error del reader
    private boolean isErrorMarker(CuentasAnuales item) {
        return item.getTransaccion() != null && item.getTransaccion().startsWith("__ERROR_MARKER__");
    }

    private CuentasAnuales crearCuentaAnualProcesada(CuentasAnuales original, String transaccionNormalizada) {
        CuentasAnuales procesada = new CuentasAnuales();
        procesada.setCuenta_id(original.getCuenta_id());
        procesada.setFecha(original.getFecha());
        procesada.setTransaccion(transaccionNormalizada);
        procesada.setDescripcion(normalizarDescripcion(original.getDescripcion()));

        // Aplicar transformaciones al monto
        BigDecimal montoTransformado = transformarMonto(original.getMonto());
        procesada.setMonto(montoTransformado);

        return procesada;
    }

    private boolean validarCamposObligatorios(CuentasAnuales item) {
        if (item.getCuenta_id() == null || item.getCuenta_id() <= 0) {
            log.debug("cuenta_id inválido: {}", item.getCuenta_id());
            return false;
        }

        if (item.getFecha() == null) {
            log.debug("Fecha nula para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (item.getTransaccion() == null || item.getTransaccion().trim().isEmpty()) {
            log.debug("Transacción nula o vacía para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (item.getMonto() == null) {
            log.debug("Monto nulo para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (item.getDescripcion() == null || item.getDescripcion().trim().isEmpty()) {
            log.debug("Descripción nula o vacía para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        return true;
    }

    private boolean validarCuentaId(CuentasAnuales item) {
        Long cuentaId = item.getCuenta_id();

        if (cuentaId == null || cuentaId <= 0) {
            log.debug("cuenta_id debe ser un número positivo: {}", cuentaId);
            return false;
        }

        // Validar rango razonable para IDs de cuenta
        if (cuentaId > 999999999L) {
            log.debug("cuenta_id fuera de rango válido: {}", cuentaId);
            return false;
        }

        return true;
    }

    private boolean validarFecha(CuentasAnuales item) {
        LocalDate fecha = item.getFecha();

        // Validar que no sea futura si está configurado
        if (processorConfig.isValidarFechasFuturas()) {
            LocalDate ahora = LocalDate.now();
            if (fecha.isAfter(ahora)) {
                log.debug("Fecha futura no permitida: {} para cuenta_id: {}", fecha, item.getCuenta_id());
                return false;
            }
        }

        // Validar año mínimo
        LocalDate fechaMinima = LocalDate.of(processorConfig.getAnioMinimo(), 1, 1);
        if (fecha.isBefore(fechaMinima)) {
            log.debug("Fecha anterior al año mínimo {}: {} para cuenta_id: {}",
                    processorConfig.getAnioMinimo(), fecha, item.getCuenta_id());
            return false;
        }

        return true;
    }

    private boolean validarMonto(CuentasAnuales item) {
        BigDecimal monto = item.getMonto();

        // Validar rangos máximos
        BigDecimal montoMaximo = new BigDecimal(processorConfig.getMontoMaximo());
        if (monto.abs().compareTo(montoMaximo) > 0) {
            log.debug("Monto excede el límite permitido: {} para cuenta_id: {}", monto, item.getCuenta_id());
            return false;
        }

        // Permitir montos cero para cuentas anuales (pueden tener movimientos de $0
        // como ajustes)
        return true;
    }

    private boolean validarDescripcion(CuentasAnuales item) {
        String descripcion = item.getDescripcion();

        if (descripcion == null || descripcion.trim().isEmpty()) {
            log.debug("Descripción vacía para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        String descripcionLimpia = descripcion.trim();

        // Validar longitud mínima
        if (descripcionLimpia.length() < 2) {
            log.debug("Descripción muy corta para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        // Validar longitud máxima
        if (descripcionLimpia.length() > 500) {
            log.debug("Descripción muy larga para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        return true;
    }

    private String normalizarDescripcion(String descripcion) {
        if (descripcion == null) {
            return null;
        }

        String descripcionLimpia = descripcion.trim();

        // Normalizar espacios múltiples
        if (processorConfig.isNormalizarEspacios()) {
            descripcionLimpia = descripcionLimpia.replaceAll("\\s+", " ");
        }

        // Capitalizar primera letra si está configurado
        if (processorConfig.isCapitalizarNombres() && descripcionLimpia.length() > 0) {
            descripcionLimpia = Character.toUpperCase(descripcionLimpia.charAt(0)) +
                    (descripcionLimpia.length() > 1 ? descripcionLimpia.substring(1).toLowerCase() : "");
        }

        return descripcionLimpia;
    }

    private BigDecimal transformarMonto(BigDecimal monto) {
        if (processorConfig.isConvertirNegativos() && monto.compareTo(BigDecimal.ZERO) < 0) {
            log.debug("Convirtiendo monto negativo {} a positivo", monto);
            return monto.abs();
        }
        return monto;
    }

    private String normalizarTipoTransaccion(String transaccion) {
        if (transaccion == null) {
            return null;
        }

        String transaccionLimpia = transaccion.trim().toUpperCase();

        // Verificar si es un tipo explícitamente inválido
        if (TIPOS_INVALIDOS.contains(transaccionLimpia)) {
            log.debug("Tipo de transacción identificado como inválido: {}", transaccion);
            return transaccionLimpia; // Retornar para que sea rechazado posteriormente
        }

        // Mapear tipos comunes a los estándar
        switch (transaccionLimpia) {
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

            case "DEP":
                return "DEPOSITO";

            case "RET":
                return "RETIRO";

            case "TRANSFERENCIA":
            case "TRANSFER":
            case "TRF":
            case "TRANSF":
                return "TRANSFERENCIA";

            case "INTERES":
            case "INTEREST":
            case "INT":
            case "INTERESES":
                return "INTERES";

            case "COMISION":
            case "COMMISSION":
            case "COM":
            case "COMISIONES":
            case "FEE":
                return "COMISION";

            case "AJUSTE":
            case "ADJUSTMENT":
            case "ADJ":
            case "AJUSTES":
                return "AJUSTE";

            case "CARGO":
            case "CHARGE":
            case "CHG":
            case "CARGOS":
                return "CARGO";

            case "ABONO":
            case "CREDIT_NOTE":
            case "ABN":
            case "ABONOS":
                return "ABONO";

            default:
                log.debug("Tipo de transacción no reconocido, será rechazado: {}", transaccion);
                return transaccionLimpia; // Retornar el tipo sin cambios para que sea rechazado
        }
    }

    // Obtiene estadísticas del procesamiento
    public void logProcessingStats() {
        log.info("=== ESTADÍSTICAS DE PROCESAMIENTO DE CUENTAS ANUALES ===");
        log.info("Total procesados en processor: {}", processedCount.get());
        log.info("Registros válidos enviados al writer: {}", validCount.get());
        log.info("Registros con errores filtrados: {}", errorCount.get());
        log.info("Marcadores de error saltados: {}", skippedCount.get());
        log.info("Archivo de errores: {}", errorWriter.getErrorFilePath());
        log.info("Tipos de transacción válidos aceptados: {}", TIPOS_VALIDOS);
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