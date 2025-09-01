package com.batch.SpringBatchApp.utils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.config.ProcessorConfig;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ValidationUtils {

    @Autowired
    private ProcessorConfig processorConfig;

    // Patrones de validación
    private static final Pattern NUMERO_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?$");
    private static final Pattern NOMBRE_PATTERN = Pattern.compile("^[a-zA-ZÀ-ÿ\\s\\-\\.]+$");
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$");

    // Tipos válidos para cada entidad
    private static final List<String> TIPOS_TRANSACCION_VALIDOS = Arrays.asList("DEBITO", "CREDITO");
    private static final List<String> TIPOS_CUENTA_VALIDOS = Arrays.asList("AHORRO", "PRESTAMO", "HIPOTECA", "CREDITO");

    // Valida si un string es un número válido
    public boolean isValidNumber(String numberStr) {
        if (numberStr == null || numberStr.trim().isEmpty()) {
            return false;
        }
        return NUMERO_PATTERN.matcher(numberStr.trim()).matches();
    }

    // Valida si un string puede convertirse a BigDecimal
    public boolean isValidDecimal(String decimalStr) {
        if (!isValidNumber(decimalStr)) {
            return false;
        }
        try {
            new BigDecimal(decimalStr.trim());
            return true;
        } catch (NumberFormatException e) {
            log.debug("String '{}' no puede convertirse a BigDecimal", decimalStr);
            return false;
        }
    }

    // Valida si un string puede convertirse a Long
    public boolean isValidLong(String longStr) {
        if (!isValidNumber(longStr)) {
            return false;
        }
        try {
            Long.parseLong(longStr.trim());
            return true;
        } catch (NumberFormatException e) {
            log.debug("String '{}' no puede convertirse a Long", longStr);
            return false;
        }
    }

    // Valida si un string puede convertirse a Integer
    public boolean isValidInteger(String intStr) {
        if (!isValidNumber(intStr)) {
            return false;
        }
        try {
            Integer.parseInt(intStr.trim());
            return true;
        } catch (NumberFormatException e) {
            log.debug("String '{}' no puede convertirse a Integer", intStr);
            return false;
        }
    }

    // Valida si un nombre tiene formato válido
    public boolean isValidName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return false;
        }

        String cleanName = name.trim();

        // Verificar longitud
        if (!processorConfig.esLongitudNombreValida(cleanName)) {
            return false;
        }

        // Verificar patrón (letras, espacios, guiones, puntos)
        return NOMBRE_PATTERN.matcher(cleanName).matches();
    }

    // Valida si un monto/saldo está dentro de los límites permitidos
    public boolean isValidAmount(BigDecimal amount) {
        if (amount == null) {
            return false;
        }

        return processorConfig.esMontoValido(amount);
    }

    // Valida si una edad está en rango válido
    public boolean isValidAge(int age) {
        return processorConfig.esEdadValida(age);
    }

    // Valida si un tipo de transacción es válido
    public boolean isValidTransactionType(String type) {
        if (type == null || type.trim().isEmpty()) {
            return false;
        }

        String normalizedType = normalizeTransactionType(type);
        return TIPOS_TRANSACCION_VALIDOS.contains(normalizedType);
    }

    // Valida si un tipo de cuenta es válido
    public boolean isValidAccountType(String type) {
        if (type == null || type.trim().isEmpty()) {
            return false;
        }

        String normalizedType = normalizeAccountType(type);
        return TIPOS_CUENTA_VALIDOS.contains(normalizedType);
    }

    // Valida si una fecha está en rango válido
    public boolean isValidDate(LocalDate date) {
        if (date == null) {
            return false;
        }

        LocalDate now = LocalDate.now();
        LocalDate minDate = LocalDate.of(processorConfig.getAnioMinimo(), 1, 1);

        // Fecha muy antigua
        if (date.isBefore(minDate)) {
            return false;
        }

        // Fecha futura (si está habilitada la validación)
        if (processorConfig.isValidarFechasFuturas() && date.isAfter(now)) {
            return false;
        }

        return true;
    }

    // Normaliza un tipo de transacción a los valores estándar
    public String normalizeTransactionType(String type) {
        if (type == null || type.trim().isEmpty()) {
            return null;
        }

        String cleanType = type.trim().toUpperCase();

        switch (cleanType) {
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
                log.debug("Tipo de transacción no reconocido: '{}'", type);
                return cleanType; // Retornar sin cambios para que sea evaluado posteriormente
        }
    }

    // Normaliza un tipo de cuenta a los valores estándar
    public String normalizeAccountType(String type) {
        if (type == null || type.trim().isEmpty()) {
            return null;
        }

        String cleanType = type.trim().toUpperCase();

        switch (cleanType) {
            case "AHORRO":
            case "SAVINGS":
            case "SAV":
            case "CUENTA_AHORRO":
            case "CUENTAAHORRO":
                return "AHORRO";

            case "PRESTAMO":
            case "LOAN":
            case "PRESTAMO_PERSONAL":
            case "PRESTAMOPERSONAL":
                return "PRESTAMO";

            case "HIPOTECA":
            case "MORTGAGE":
            case "HIP":
            case "CREDITO_HIPOTECARIO":
            case "CREDITOHIPOTECARIO":
                return "HIPOTECA";

            case "CREDITO":
            case "CREDIT":
            case "CORRIENTE":
            case "CHECKING":
            case "CURRENT":
            case "CHK":
                return "CREDITO";

            default:
                log.debug("Tipo de cuenta no reconocido: '{}'", type);
                return cleanType; // Retornar sin cambios para que sea evaluado posteriormente
        }
    }

    // Valida si un string no está vacío ni es solo espacios
    public boolean isNotEmpty(String str) {
        return str != null && !str.trim().isEmpty();
    }

    // Valida si un ID es válido (positivo y dentro de rango)
    public boolean isValidId(Long id) {
        return id != null && id > 0 && id <= 999999999L;
    }

    // Limpia y normaliza un string removiendo espacios extra y caracteres
    // especiales
    public String cleanString(String input) {
        if (input == null) {
            return null;
        }

        String cleaned = input.trim();

        if (processorConfig.isNormalizarEspacios()) {
            // Reemplazar múltiples espacios con uno solo
            cleaned = cleaned.replaceAll("\\s+", " ");
        }

        return cleaned;
    }

    // Capitaliza un nombre según la configuración
    public String capitalizeName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return name;
        }

        if (!processorConfig.isCapitalizarNombres()) {
            return name.trim();
        }

        String[] words = name.trim().toLowerCase().split("\\s+");
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < words.length; i++) {
            if (i > 0) {
                result.append(" ");
            }
            if (words[i].length() > 0) {
                result.append(Character.toUpperCase(words[i].charAt(0)));
                if (words[i].length() > 1) {
                    result.append(words[i].substring(1));
                }
            }
        }

        return result.toString();
    }

    // Valida un email básico (si fuera necesario en futuras expansiones)
    public boolean isValidEmail(String email) {
        if (email == null || email.trim().isEmpty()) {
            return false;
        }
        return EMAIL_PATTERN.matcher(email.trim()).matches();
    }

    // Convierte un monto negativo a positivo si está configurado
    public BigDecimal processAmount(BigDecimal amount) {
        if (amount == null) {
            return null;
        }

        if (processorConfig.isConvertirNegativos() && amount.compareTo(BigDecimal.ZERO) < 0) {
            log.debug("Convirtiendo monto negativo {} a positivo", amount);
            return amount.abs();
        }

        return amount;
    }

    // Obtiene los tipos válidos para transacciones
    public List<String> getValidTransactionTypes() {
        return TIPOS_TRANSACCION_VALIDOS;
    }

    // Obtiene los tipos válidos para cuentas
    public List<String> getValidAccountTypes() {
        return TIPOS_CUENTA_VALIDOS;
    }

    // Valida múltiples campos obligatorios de una vez
    public boolean areRequiredFieldsValid(Object... fields) {
        for (Object field : fields) {
            if (field == null) {
                return false;
            }
            if (field instanceof String && ((String) field).trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }
}