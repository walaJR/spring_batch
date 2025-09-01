package com.batch.SpringBatchApp.steps;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.config.ProcessorConfig;
import com.batch.SpringBatchApp.entities.Intereses;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InteresesItemProcessor implements ItemProcessor<Intereses, Intereses> {

    // Tipos de cuenta válidos para intereses
    private static final List<String> TIPOS_VALIDOS = Arrays.asList("AHORRO", "PRESTAMO", "HIPOTECA", "CREDITO");

    // Tipos que deben ser enviados al archivo de errores
    private static final List<String> TIPOS_INVALIDOS = Arrays.asList("-1", "INVALID", "DESCONOCIDO", "UNKNOWN",
            "ERROR");

    @Autowired
    private ProcessorConfig processorConfig;

    @Autowired
    private ErrorInteresesWriter errorWriter;

    // Contadores para estadísticas
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong validCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong skippedCount = new AtomicLong(0);

    @Override
    @Nullable
    public Intereses process(@NonNull Intereses item) throws Exception {
        processedCount.incrementAndGet();

        log.debug("Procesando registro de interés: cuenta_id={}, tipo={}", item.getCuenta_id(), item.getTipo());

        // Verificar si es un marcador de error del reader
        if (isErrorMarker(item)) {
            log.debug("Marcador de error detectado, saltando: cuenta_id={}", item.getCuenta_id());
            skippedCount.incrementAndGet();
            return null; // Filtrar - no procesar más
        }

        try {
            // Validación de campos obligatorios
            if (!validarCamposObligatorios(item)) {
                errorWriter.writeErrorInteres(item, "Campos obligatorios faltantes", "N/A");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de cuenta_id
            if (!validarCuentaId(item)) {
                errorWriter.writeErrorInteres(item, "cuenta_id inválido",
                        item.getCuenta_id() != null ? item.getCuenta_id().toString() : "null");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de nombre
            if (!validarNombre(item)) {
                errorWriter.writeErrorInteres(item, "Nombre inválido", item.getNombre());
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de saldo
            if (!validarSaldo(item)) {
                errorWriter.writeErrorInteres(item, "Saldo inválido",
                        item.getSaldo() != null ? item.getSaldo().toString() : "null");
                errorCount.incrementAndGet();
                return null;
            }

            // Validación de edad
            if (!validarEdad(item)) {
                errorWriter.writeErrorInteres(item, "Edad inválida", String.valueOf(item.getEdad()));
                errorCount.incrementAndGet();
                return null;
            }

            // Validación crítica de tipo - RECHAZAR tipos inválidos
            String tipoNormalizado = normalizarTipo(item.getTipo());
            if (!TIPOS_VALIDOS.contains(tipoNormalizado)) {
                log.warn(
                        "Registro rechazado por tipo inválido: cuenta_id={}, Tipo original='{}', Tipo normalizado='{}'",
                        item.getCuenta_id(), item.getTipo(), tipoNormalizado);
                errorWriter.writeErrorInteres(item, "Tipo de cuenta no válido", item.getTipo());
                errorCount.incrementAndGet();
                return null;
            }

            // Si llegamos aquí, el registro es válido - crear versión procesada
            Intereses interesProcesado = crearInteresProcesado(item, tipoNormalizado);
            validCount.incrementAndGet();

            log.info("Registro de interés procesado exitosamente: cuenta_id={}, nombre={}, saldo={}, edad={}, tipo={}",
                    interesProcesado.getCuenta_id(), interesProcesado.getNombre(),
                    interesProcesado.getSaldo(), interesProcesado.getEdad(), interesProcesado.getTipo());

            return interesProcesado;

        } catch (Exception e) {
            log.error("Error inesperado al procesar registro de interés cuenta_id={}: {}", item.getCuenta_id(),
                    e.getMessage(), e);
            errorWriter.writeErrorInteres(item, "Error de procesamiento: " + e.getMessage(), "N/A");
            errorCount.incrementAndGet();
            return null; // Filtrar en lugar de lanzar excepción
        }
    }

    // Verifica si el registro es un marcador de error del reader
    private boolean isErrorMarker(Intereses item) {
        return item.getTipo() != null && item.getTipo().startsWith("__ERROR_MARKER__");
    }

    private Intereses crearInteresProcesado(Intereses original, String tipoNormalizado) {
        Intereses procesado = new Intereses();
        procesado.setCuenta_id(original.getCuenta_id());
        procesado.setNombre(normalizarNombre(original.getNombre()));
        procesado.setEdad(original.getEdad());
        procesado.setTipo(tipoNormalizado);

        // Aplicar transformaciones al saldo
        BigDecimal saldoTransformado = transformarSaldo(original.getSaldo());
        procesado.setSaldo(saldoTransformado);

        return procesado;
    }

    private boolean validarCamposObligatorios(Intereses item) {
        if (item.getCuenta_id() == null || item.getCuenta_id() <= 0) {
            log.debug("cuenta_id inválido: {}", item.getCuenta_id());
            return false;
        }

        if (item.getNombre() == null || item.getNombre().trim().isEmpty()) {
            log.debug("Nombre nulo o vacío para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (item.getSaldo() == null) {
            log.debug("Saldo nulo para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (item.getEdad() < 0) {
            log.debug("Edad negativa para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (item.getTipo() == null || item.getTipo().trim().isEmpty()) {
            log.debug("Tipo nulo o vacío para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        return true;
    }

    private boolean validarCuentaId(Intereses item) {
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

    private boolean validarNombre(Intereses item) {
        String nombre = item.getNombre();

        if (nombre == null || nombre.trim().isEmpty()) {
            log.debug("Nombre vacío para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        String nombreLimpio = nombre.trim();

        // Validar longitud
        if (nombreLimpio.length() < 2) {
            log.debug("Nombre muy corto para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (nombreLimpio.length() > 100) {
            log.debug("Nombre muy largo para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        // Validar que contenga al menos una letra
        if (!nombreLimpio.matches(".*[a-zA-ZÀ-ÿ].*")) {
            log.debug("Nombre debe contener al menos una letra para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        return true;
    }

    private boolean validarSaldo(Intereses item) {
        BigDecimal saldo = item.getSaldo();

        // Validar rangos máximos
        BigDecimal saldoMaximo = new BigDecimal(processorConfig.getMontoMaximo());
        if (saldo.abs().compareTo(saldoMaximo) > 0) {
            log.debug("Saldo excede el límite permitido: {} para cuenta_id: {}", saldo, item.getCuenta_id());
            return false;
        }

        // Permitir saldos cero para cuentas de interés (pueden estar inactivas)
        return true;
    }

    private boolean validarEdad(Intereses item) {
        int edad = item.getEdad();

        // Validar rango de edad razonable
        if (edad < 0) {
            log.debug("Edad no puede ser negativa para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        if (edad > 150) {
            log.debug("Edad muy alta para cuenta_id: {}", item.getCuenta_id());
            return false;
        }

        return true;
    }

    private String normalizarNombre(String nombre) {
        if (nombre == null) {
            return null;
        }

        String nombreLimpio = nombre.trim();

        // Capitalizar primera letra de cada palabra
        String[] palabras = nombreLimpio.toLowerCase().split("\\s+");
        StringBuilder resultado = new StringBuilder();

        for (int i = 0; i < palabras.length; i++) {
            if (i > 0) {
                resultado.append(" ");
            }
            if (palabras[i].length() > 0) {
                resultado.append(Character.toUpperCase(palabras[i].charAt(0)));
                if (palabras[i].length() > 1) {
                    resultado.append(palabras[i].substring(1));
                }
            }
        }

        return resultado.toString();
    }

    private BigDecimal transformarSaldo(BigDecimal saldo) {
        if (processorConfig.isConvertirNegativos() && saldo.compareTo(BigDecimal.ZERO) < 0) {
            log.debug("Convirtiendo saldo negativo {} a positivo", saldo);
            return saldo.abs();
        }
        return saldo;
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

        // Mapear tipos comunes a los estándar
        switch (tipoLimpio) {
            case "AHORRO":
            case "SAVINGS":
            case "SAV":
            case "CUENTA_AHORRO":
            case "CUENTAAHORRO":
                return "AHORRO";

            case "PRESTAMO":
            case "LOAN":
            case "CREDITO":
            case "CREDIT":
            case "CREDITO_PERSONAL":
            case "CREDITOPERSONAL":
                return "PRESTAMO";

            case "HIPOTECA":
            case "MORTGAGE":
            case "HIP":
            case "CREDITO_HIPOTECARIO":
            case "CREDITOHIPOTECARIO":
                return "HIPOTECA";

            case "CORRIENTE":
            case "CHECKING":
            case "CURRENT":
            case "CHK":
                return "CREDITO"; // Mapear cuenta corriente a crédito

            default:
                log.debug("Tipo no reconocido, será rechazado: {}", tipo);
                return tipoLimpio; // Retornar el tipo sin cambios para que sea rechazado
        }
    }

    // Obtiene estadísticas del procesamiento
    public void logProcessingStats() {
        log.info("=== ESTADÍSTICAS DE PROCESAMIENTO DE INTERESES ===");
        log.info("Total procesados en processor: {}", processedCount.get());
        log.info("Registros válidos enviados al writer: {}", validCount.get());
        log.info("Registros con errores filtrados: {}", errorCount.get());
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