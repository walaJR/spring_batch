package com.batch.SpringBatchApp.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DateParser {

    // Lista de formatos de fecha soportados en orden de prioridad
    private static final List<DateTimeFormatter> FORMATTERS = Arrays.asList(
            DateTimeFormatter.ofPattern("yyyy-MM-dd"), // 2024-01-05
            DateTimeFormatter.ofPattern("dd-MM-yyyy"), // 05-01-2024
            DateTimeFormatter.ofPattern("MM-dd-yyyy"), // 01-05-2024
            DateTimeFormatter.ofPattern("dd/MM/yyyy"), // 05/01/2024
            DateTimeFormatter.ofPattern("MM/dd/yyyy"), // 01/05/2024
            DateTimeFormatter.ofPattern("yyyy/MM/dd"), // 2024/01/05
            DateTimeFormatter.ofPattern("yyyy-MM-d"), // 2024-01-5
            DateTimeFormatter.ofPattern("yyyy-M-dd"), // 2024-1-05
            DateTimeFormatter.ofPattern("yyyy-M-d"), // 2024-1-5
            DateTimeFormatter.ofPattern("dd-MM-yy"), // 05-01-24
            DateTimeFormatter.ofPattern("MM-dd-yy"), // 01-05-24
            DateTimeFormatter.ofPattern("dd/MM/yy"), // 05/01/24
            DateTimeFormatter.ofPattern("MM/dd/yy") // 01/05/24
    );

    // Intenta parsear una fecha usando múltiples formatos
    public LocalDate parseDate(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            log.debug("Cadena de fecha vacía o nula");
            return null;
        }

        String cleanDateString = dateString.trim();

        for (DateTimeFormatter formatter : FORMATTERS) {
            try {
                LocalDate date = LocalDate.parse(cleanDateString, formatter);

                // Validación adicional para años de 2 dígitos
                if (date.getYear() < 100) {
                    // Asumimos que años 00-30 son 2000-2030, y 31-99 son 1931-1999
                    int adjustedYear = date.getYear() < 30 ? 2000 + date.getYear() : 1900 + date.getYear();
                    date = date.withYear(adjustedYear);
                }

                log.debug("Fecha parseada exitosamente: '{}' -> {}", cleanDateString, date);
                return date;

            } catch (DateTimeParseException e) {
                log.trace("Formato {} no coincide para fecha: {}", formatter.toString(), cleanDateString);
                continue;
            }
        }

        log.warn("No se pudo parsear la fecha: '{}'", cleanDateString);
        return null;
    }

    // Valida si una fecha es válida para el procesamiento
    public boolean isValidDate(LocalDate date) {
        if (date == null) {
            return false;
        }

        LocalDate now = LocalDate.now();
        LocalDate minDate = LocalDate.of(2000, 1, 1);
        LocalDate maxDate = now.plusDays(30); // Permitir hasta 30 días en el futuro

        boolean isValid = !date.isBefore(minDate) && !date.isAfter(maxDate);

        if (!isValid) {
            log.debug("Fecha fuera del rango válido: {} (rango: {} a {})", date, minDate, maxDate);
        }

        return isValid;
    }

    // Normaliza una fecha al formato estándar yyyy-MM-dd
    public String normalizeDate(LocalDate date) {
        if (date == null) {
            return null;
        }
        return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }
}