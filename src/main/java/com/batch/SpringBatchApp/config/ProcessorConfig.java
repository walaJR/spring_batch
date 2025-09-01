package com.batch.SpringBatchApp.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "batch.processor")
@Data
public class ProcessorConfig {

    // === CONFIGURACIÓN COMÚN ===

    // Límite máximo permitido para montos/saldos
    private String montoMaximo = "1000000";

    // Si debe convertir montos/saldos negativos a positivos
    private boolean convertirNegativos = true;

    // Si debe normalizar tipos (tanto transacciones como cuentas)
    private boolean normalizarTipos = true;

    // === CONFIGURACIÓN ESPECÍFICA PARA TRANSACCIONES ===

    // Año mínimo permitido para fechas de transacciones
    private int anioMinimo = 2000;

    // Si debe validar fechas futuras en transacciones
    private boolean validarFechasFuturas = true;

    // Tipo por defecto cuando no se puede mapear un tipo de transacción
    private String tipoTransaccionPorDefecto = "CREDITO";

    // Si debe omitir transacciones con montos cero
    private boolean omitirMontosCero = true;

    // === CONFIGURACIÓN ESPECÍFICA PARA INTERESES ===

    // Tipo por defecto cuando no se puede mapear un tipo de cuenta de interés
    private String tipoCuentaPorDefecto = "AHORRO";

    // Si debe omitir cuentas de interés con saldos cero (false para intereses,
    // pueden tener saldo 0)
    private boolean omitirSaldosCero = false;

    // Edad mínima permitida para cuentas de interés
    private int edadMinima = 0;

    // Edad máxima permitida para cuentas de interés
    private int edadMaxima = 150;

    // Longitud mínima para nombres en cuentas de interés
    private int longitudMinimaNombre = 2;

    // Longitud máxima para nombres en cuentas de interés
    private int longitudMaximaNombre = 100;

    // Si debe capitalizar nombres automáticamente
    private boolean capitalizarNombres = true;

    // === CONFIGURACIÓN DE TOLERANCIA A ERRORES ===

    // Límite máximo de errores antes de fallar el job
    private int limiteErrores = 1000;

    // Número de reintentos para errores recuperables
    private int numeroReintentos = 3;

    // Tamaño del chunk para procesamiento por lotes
    private int tamanoChunk = 50;

    // === CONFIGURACIÓN DE ARCHIVOS DE ERROR ===

    // Directorio donde se guardan los archivos de error
    private String directorioErrores = "error-files";

    // Si debe incluir timestamp en nombres de archivos de error
    private boolean incluirTimestampEnErrores = true;

    // === CONFIGURACIÓN DE LOGGING ===

    // Nivel de detalle en logs (DEBUG, INFO, WARN, ERROR)
    private String nivelLogDetalle = "INFO";

    // Si debe mostrar estadísticas detalladas al final del procesamiento
    private boolean mostrarEstadisticasDetalladas = true;

    // === MÉTODOS DE CONVENIENCIA ===

    // Verifica si un monto está dentro del rango permitido
    public boolean esMontoValido(java.math.BigDecimal monto) {
        if (monto == null)
            return false;
        java.math.BigDecimal limite = new java.math.BigDecimal(montoMaximo);
        return monto.abs().compareTo(limite) <= 0;
    }

    // Verifica si una edad está dentro del rango permitido
    public boolean esEdadValida(int edad) {
        return edad >= edadMinima && edad <= edadMaxima;
    }

    // Verifica si la longitud de un nombre es válida
    public boolean esLongitudNombreValida(String nombre) {
        if (nombre == null)
            return false;
        int longitud = nombre.trim().length();
        return longitud >= longitudMinimaNombre && longitud <= longitudMaximaNombre;
    }

    // Verifica si un año está dentro del rango permitido para fechas
    public boolean esAnioValido(int anio) {
        int anioActual = java.time.LocalDate.now().getYear();
        return anio >= anioMinimo && anio <= anioActual + (validarFechasFuturas ? 0 : 10);
    }

    // Obtiene el límite máximo como BigDecimal para comparaciones
    public java.math.BigDecimal getLimiteMaximoComoDecimal() {
        return new java.math.BigDecimal(montoMaximo);
    }

    // === CONFIGURACIÓN ADICIONAL PARA VALIDACIONES ESTRICTAS ===

    // Si debe aplicar validaciones estrictas (más restrictivas)
    private boolean validacionesEstrictas = false;

    // Si debe rechazar automáticamente registros duplicados por ID
    private boolean rechazarDuplicados = false;

    // Si debe normalizar automáticamente espacios en blanco
    private boolean normalizarEspacios = true;

    // Si debe convertir texto a mayúsculas/minúsculas según el tipo
    private boolean normalizarCase = true;
}