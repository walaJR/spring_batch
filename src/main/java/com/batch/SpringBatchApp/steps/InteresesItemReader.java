package com.batch.SpringBatchApp.steps;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.entities.Intereses;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InteresesItemReader extends FlatFileItemReader<Intereses> {

    @Autowired
    private ErrorInteresesWriter errorWriter;

    public InteresesItemReader() {
        setName("readIntereses");
        setResource(new ClassPathResource("intereses.csv"));
        setLinesToSkip(1); // Saltar header
        setEncoding(StandardCharsets.UTF_8.name());
    }

    @Autowired
    public void configureAfterPropertiesSet() {
        setLineMapper(getLineMapper());
    }

    private LineMapper<Intereses> getLineMapper() {
        DefaultLineMapper<Intereses> lineMapper = new DefaultLineMapper<>();

        // Configurar tokenizer
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("cuenta_id", "nombre", "saldo", "edad", "tipo");
        lineTokenizer.setDelimiter(",");

        // Configurar mapper personalizado que NO lanza excepciones
        FieldSetMapper<Intereses> fieldSetMapper = new FieldSetMapper<Intereses>() {
            @Override
            public Intereses mapFieldSet(FieldSet fieldSet) {
                String cuentaIdStr = null;
                String nombreStr = null;
                String saldoStr = null;
                String edadStr = null;
                String tipoStr = null;

                try {
                    // Extraer valores del FieldSet
                    cuentaIdStr = fieldSet.readString("cuenta_id");
                    nombreStr = fieldSet.readString("nombre");
                    saldoStr = fieldSet.readString("saldo");
                    edadStr = fieldSet.readString("edad");
                    tipoStr = fieldSet.readString("tipo");

                    // Crear registro de interés
                    Intereses interes = new Intereses();

                    // Procesar cuenta_id
                    if (cuentaIdStr == null || cuentaIdStr.trim().isEmpty()) {
                        log.warn("cuenta_id vacío o nulo en línea");
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                "cuenta_id vacío o nulo");
                        return createErrorMarker(-1L, "cuenta_id inválido");
                    }

                    Long cuentaId;
                    try {
                        cuentaId = Long.parseLong(cuentaIdStr.trim());
                        if (cuentaId <= 0) {
                            log.warn("cuenta_id debe ser positivo: '{}'", cuentaIdStr);
                            errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                    "cuenta_id debe ser positivo");
                            return createErrorMarker(-1L, "cuenta_id inválido");
                        }
                        interes.setCuenta_id(cuentaId);
                    } catch (NumberFormatException e) {
                        log.warn("cuenta_id con formato inválido: '{}'", cuentaIdStr);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                "Formato de cuenta_id inválido");
                        return createErrorMarker(-1L, "Formato de cuenta_id inválido");
                    }

                    // Procesar nombre
                    if (nombreStr == null || nombreStr.trim().isEmpty()) {
                        log.warn("Nombre vacío para cuenta_id {}", cuentaId);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr, "Nombre vacío");
                        return createErrorMarker(cuentaId, "Nombre vacío");
                    }

                    // Validar nombre - solo letras, espacios y caracteres básicos
                    String nombreLimpio = nombreStr.trim();
                    if (nombreLimpio.length() < 2) {
                        log.warn("Nombre muy corto para cuenta_id {}: '{}'", cuentaId, nombreStr);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                "Nombre muy corto (mínimo 2 caracteres)");
                        return createErrorMarker(cuentaId, "Nombre muy corto");
                    }

                    if (nombreLimpio.length() > 100) {
                        log.warn("Nombre muy largo para cuenta_id {}: '{}'", cuentaId, nombreStr);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                "Nombre muy largo (máximo 100 caracteres)");
                        return createErrorMarker(cuentaId, "Nombre muy largo");
                    }

                    interes.setNombre(nombreLimpio);

                    // Procesar saldo
                    if (saldoStr == null || saldoStr.trim().isEmpty()) {
                        log.warn("Saldo vacío para cuenta_id {}", cuentaId);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr, "Saldo vacío");
                        return createErrorMarker(cuentaId, "Saldo vacío");
                    }

                    try {
                        BigDecimal saldo = new BigDecimal(saldoStr.trim());
                        interes.setSaldo(saldo);
                    } catch (NumberFormatException e) {
                        log.warn("Formato de saldo inválido para cuenta_id {}: '{}'", cuentaId, saldoStr);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                "Formato de saldo inválido");
                        return createErrorMarker(cuentaId, "Formato de saldo inválido");
                    }

                    // Procesar edad
                    if (edadStr == null || edadStr.trim().isEmpty()) {
                        log.warn("Edad vacía para cuenta_id {}", cuentaId);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr, "Edad vacía");
                        return createErrorMarker(cuentaId, "Edad vacía");
                    }

                    try {
                        int edad = Integer.parseInt(edadStr.trim());
                        if (edad < 0) {
                            log.warn("Edad negativa para cuenta_id {}: '{}'", cuentaId, edadStr);
                            errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                    "Edad no puede ser negativa");
                            return createErrorMarker(cuentaId, "Edad negativa");
                        }
                        if (edad > 150) {
                            log.warn("Edad muy alta para cuenta_id {}: '{}'", cuentaId, edadStr);
                            errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                    "Edad muy alta (máximo 150 años)");
                            return createErrorMarker(cuentaId, "Edad muy alta");
                        }
                        interes.setEdad(edad);
                    } catch (NumberFormatException e) {
                        log.warn("Formato de edad inválido para cuenta_id {}: '{}'", cuentaId, edadStr);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                "Formato de edad inválido");
                        return createErrorMarker(cuentaId, "Formato de edad inválido");
                    }

                    // Procesar tipo
                    if (tipoStr == null || tipoStr.trim().isEmpty()) {
                        log.warn("Tipo vacío para cuenta_id {}", cuentaId);
                        errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr,
                                "Tipo de cuenta vacío");
                        return createErrorMarker(cuentaId, "Tipo vacío");
                    }

                    interes.setTipo(tipoStr.trim());

                    log.debug(
                            "Registro de interés mapeado exitosamente en reader: cuenta_id={}, nombre={}, saldo={}, edad={}, tipo={}",
                            cuentaId, nombreLimpio, interes.getSaldo(), interes.getEdad(), tipoStr.trim());

                    return interes;

                } catch (Exception e) {
                    String errorMsg = "Error general en mapeo: " + e.getMessage();
                    log.error("Error inesperado al mapear línea de intereses: {}", fieldSet.toString(), e);
                    errorWriter.writeErrorLine(cuentaIdStr, nombreStr, saldoStr, edadStr, tipoStr, errorMsg);
                    return createErrorMarker(-1L, "Error de mapeo general");
                }
            }
        };

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    // Crea un registro de interés marcador para indicar error que será filtrado en
    // el processor
    private Intereses createErrorMarker(Long cuentaId, String reason) {
        Intereses errorMarker = new Intereses();
        errorMarker.setCuenta_id(cuentaId);
        errorMarker.setTipo("__ERROR_MARKER__:" + reason);
        return errorMarker;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (errorWriter != null) {
            setLineMapper(getLineMapper());
        }
    }
}