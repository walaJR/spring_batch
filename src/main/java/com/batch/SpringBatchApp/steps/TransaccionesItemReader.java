package com.batch.SpringBatchApp.steps;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.entities.Transacciones;
import com.batch.SpringBatchApp.utils.DateParser;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TransaccionesItemReader extends FlatFileItemReader<Transacciones> {

    @Autowired
    private DateParser dateParser;

    @Autowired
    private ErrorTransactionWriter errorWriter;

    public TransaccionesItemReader() {
        setName("readTransactions");
        setResource(new ClassPathResource("transacciones.csv"));
        setLinesToSkip(1); // Saltar header
        setEncoding(StandardCharsets.UTF_8.name());
    }

    @Autowired
    public void configureAfterPropertiesSet() {
        setLineMapper(getLineMapper());
    }

    private LineMapper<Transacciones> getLineMapper() {
        DefaultLineMapper<Transacciones> lineMapper = new DefaultLineMapper<>();

        // Configurar tokenizer
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("id", "fecha", "monto", "tipo");
        lineTokenizer.setDelimiter(",");

        // Configurar mapper personalizado que NO lanza excepciones
        FieldSetMapper<Transacciones> fieldSetMapper = new FieldSetMapper<Transacciones>() {
            @Override
            public Transacciones mapFieldSet(FieldSet fieldSet) {
                String idStr = null;
                String fechaStr = null;
                String montoStr = null;
                String tipoStr = null;

                try {
                    // Extraer valores del FieldSet
                    idStr = fieldSet.readString("id");
                    fechaStr = fieldSet.readString("fecha");
                    montoStr = fieldSet.readString("monto");
                    tipoStr = fieldSet.readString("tipo");

                    // Crear transacción marcadora de error si hay problemas
                    Transacciones transaccion = new Transacciones();
                    boolean hasError = false;
                    String errorReason = "";

                    // Procesar ID
                    if (idStr == null || idStr.trim().isEmpty()) {
                        log.warn("ID vacío o nulo en línea");
                        errorWriter.writeErrorLine(idStr, fechaStr, montoStr, tipoStr, "ID vacío o nulo");
                        return createErrorMarker(-1L, "ID inválido");
                    }

                    Long id;
                    try {
                        id = Long.parseLong(idStr.trim());
                        transaccion.setId(id);
                    } catch (NumberFormatException e) {
                        log.warn("ID con formato inválido: '{}'", idStr);
                        errorWriter.writeErrorLine(idStr, fechaStr, montoStr, tipoStr, "Formato de ID inválido");
                        return createErrorMarker(-1L, "Formato de ID inválido");
                    }

                    // Procesar fecha
                    LocalDate fecha = dateParser.parseDate(fechaStr);
                    if (fecha == null || !dateParser.isValidDate(fecha)) {
                        log.warn("Fecha inválida para ID {}: '{}'", id, fechaStr);
                        errorWriter.writeErrorLine(idStr, fechaStr, montoStr, tipoStr,
                                "Fecha inválida o fuera de rango");
                        return createErrorMarker(id, "Fecha inválida");
                    }
                    transaccion.setFecha(fecha);

                    // Procesar monto
                    if (montoStr == null || montoStr.trim().isEmpty()) {
                        log.warn("Monto vacío para ID {}", id);
                        errorWriter.writeErrorLine(idStr, fechaStr, montoStr, tipoStr, "Monto vacío");
                        return createErrorMarker(id, "Monto vacío");
                    }

                    try {
                        BigDecimal monto = new BigDecimal(montoStr.trim());
                        transaccion.setMonto(monto);
                    } catch (NumberFormatException e) {
                        log.warn("Formato de monto inválido para ID {}: '{}'", id, montoStr);
                        errorWriter.writeErrorLine(idStr, fechaStr, montoStr, tipoStr, "Formato de monto inválido");
                        return createErrorMarker(id, "Formato de monto inválido");
                    }

                    // Procesar tipo
                    if (tipoStr == null || tipoStr.trim().isEmpty()) {
                        log.warn("Tipo vacío para ID {}", id);
                        errorWriter.writeErrorLine(idStr, fechaStr, montoStr, tipoStr, "Tipo de transacción vacío");
                        return createErrorMarker(id, "Tipo vacío");
                    }

                    transaccion.setTipo(tipoStr.trim());

                    log.debug("Transacción mapeada exitosamente en reader: ID={}, Fecha={}, Monto={}, Tipo={}",
                            id, fecha, transaccion.getMonto(), tipoStr);

                    return transaccion;

                } catch (Exception e) {
                    String errorMsg = "Error general en mapeo: " + e.getMessage();
                    log.error("Error inesperado al mapear línea: {}", fieldSet.toString(), e);
                    errorWriter.writeErrorLine(idStr, fechaStr, montoStr, tipoStr, errorMsg);
                    return createErrorMarker(-1L, "Error de mapeo general");
                }
            }
        };

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    // Crea una transacción marcadora para indicar error que será filtrada en el
    // processor
    private Transacciones createErrorMarker(Long id, String reason) {
        Transacciones errorMarker = new Transacciones();
        errorMarker.setId(id);
        errorMarker.setTipo("__ERROR_MARKER__:" + reason);
        return errorMarker;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (dateParser != null && errorWriter != null) {
            setLineMapper(getLineMapper());
        }
    }
}