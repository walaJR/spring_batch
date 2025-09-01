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

import com.batch.SpringBatchApp.entities.CuentasAnuales;
import com.batch.SpringBatchApp.utils.DateParser;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CuentasAnualesItemReader extends FlatFileItemReader<CuentasAnuales> {

    @Autowired
    private DateParser dateParser;

    @Autowired
    private ErrorCuentasAnualesWriter errorWriter;

    public CuentasAnualesItemReader() {
        setName("readCuentasAnuales");
        setResource(new ClassPathResource("cuentas_anuales.csv"));
        setLinesToSkip(1); // Saltar header
        setEncoding(StandardCharsets.UTF_8.name());
    }

    @Autowired
    public void configureAfterPropertiesSet() {
        setLineMapper(getLineMapper());
    }

    private LineMapper<CuentasAnuales> getLineMapper() {
        DefaultLineMapper<CuentasAnuales> lineMapper = new DefaultLineMapper<>();

        // Configurar tokenizer
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("cuenta_id", "fecha", "transaccion", "monto", "descripcion");
        lineTokenizer.setDelimiter(",");

        // Configurar mapper personalizado que NO lanza excepciones
        FieldSetMapper<CuentasAnuales> fieldSetMapper = new FieldSetMapper<CuentasAnuales>() {
            @Override
            public CuentasAnuales mapFieldSet(FieldSet fieldSet) {
                String cuentaIdStr = null;
                String fechaStr = null;
                String transaccionStr = null;
                String montoStr = null;
                String descripcionStr = null;

                try {
                    // Extraer valores del FieldSet
                    cuentaIdStr = fieldSet.readString("cuenta_id");
                    fechaStr = fieldSet.readString("fecha");
                    transaccionStr = fieldSet.readString("transaccion");
                    montoStr = fieldSet.readString("monto");
                    descripcionStr = fieldSet.readString("descripcion");

                    // Crear registro de cuentas anuales
                    CuentasAnuales cuentaAnual = new CuentasAnuales();

                    // Procesar cuenta_id
                    if (cuentaIdStr == null || cuentaIdStr.trim().isEmpty()) {
                        log.warn("cuenta_id vacío o nulo en línea");
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "cuenta_id vacío o nulo");
                        return createErrorMarker(-1L, "cuenta_id inválido");
                    }

                    Long cuentaId;
                    try {
                        cuentaId = Long.parseLong(cuentaIdStr.trim());
                        if (cuentaId <= 0) {
                            log.warn("cuenta_id debe ser positivo: '{}'", cuentaIdStr);
                            errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                    "cuenta_id debe ser positivo");
                            return createErrorMarker(-1L, "cuenta_id inválido");
                        }
                        cuentaAnual.setCuenta_id(cuentaId);
                    } catch (NumberFormatException e) {
                        log.warn("cuenta_id con formato inválido: '{}'", cuentaIdStr);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Formato de cuenta_id inválido");
                        return createErrorMarker(-1L, "Formato de cuenta_id inválido");
                    }

                    // Procesar fecha
                    LocalDate fecha = dateParser.parseDate(fechaStr);
                    if (fecha == null || !dateParser.isValidDate(fecha)) {
                        log.warn("Fecha inválida para cuenta_id {}: '{}'", cuentaId, fechaStr);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Fecha inválida o fuera de rango");
                        return createErrorMarker(cuentaId, "Fecha inválida");
                    }
                    cuentaAnual.setFecha(fecha);

                    // Procesar transacción
                    if (transaccionStr == null || transaccionStr.trim().isEmpty()) {
                        log.warn("Transacción vacía para cuenta_id {}", cuentaId);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Tipo de transacción vacío");
                        return createErrorMarker(cuentaId, "Transacción vacía");
                    }

                    cuentaAnual.setTransaccion(transaccionStr.trim());

                    // Procesar monto
                    if (montoStr == null || montoStr.trim().isEmpty()) {
                        log.warn("Monto vacío para cuenta_id {}", cuentaId);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Monto vacío");
                        return createErrorMarker(cuentaId, "Monto vacío");
                    }

                    try {
                        BigDecimal monto = new BigDecimal(montoStr.trim());
                        cuentaAnual.setMonto(monto);
                    } catch (NumberFormatException e) {
                        log.warn("Formato de monto inválido para cuenta_id {}: '{}'", cuentaId, montoStr);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Formato de monto inválido");
                        return createErrorMarker(cuentaId, "Formato de monto inválido");
                    }

                    // Procesar descripción
                    if (descripcionStr == null || descripcionStr.trim().isEmpty()) {
                        log.warn("Descripción vacía para cuenta_id {}", cuentaId);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Descripción vacía");
                        return createErrorMarker(cuentaId, "Descripción vacía");
                    }

                    // Validar longitud de descripción
                    String descripcionLimpia = descripcionStr.trim();
                    if (descripcionLimpia.length() < 2) {
                        log.warn("Descripción muy corta para cuenta_id {}: '{}'", cuentaId, descripcionStr);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Descripción muy corta (mínimo 2 caracteres)");
                        return createErrorMarker(cuentaId, "Descripción muy corta");
                    }

                    if (descripcionLimpia.length() > 500) {
                        log.warn("Descripción muy larga para cuenta_id {}: '{}'", cuentaId, descripcionStr);
                        errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                                "Descripción muy larga (máximo 500 caracteres)");
                        return createErrorMarker(cuentaId, "Descripción muy larga");
                    }

                    cuentaAnual.setDescripcion(descripcionLimpia);

                    log.debug(
                            "Registro de cuenta anual mapeado exitosamente en reader: cuenta_id={}, fecha={}, transaccion={}, monto={}, descripcion={}",
                            cuentaId, fecha, transaccionStr.trim(), cuentaAnual.getMonto(), descripcionLimpia);

                    return cuentaAnual;

                } catch (Exception e) {
                    String errorMsg = "Error general en mapeo: " + e.getMessage();
                    log.error("Error inesperado al mapear línea de cuentas anuales: {}", fieldSet.toString(), e);
                    errorWriter.writeErrorLine(cuentaIdStr, fechaStr, transaccionStr, montoStr, descripcionStr,
                            errorMsg);
                    return createErrorMarker(-1L, "Error de mapeo general");
                }
            }
        };

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    // Crea un registro de cuenta anual marcador para indicar error que será
    // filtrado en el processor
    private CuentasAnuales createErrorMarker(Long cuentaId, String reason) {
        CuentasAnuales errorMarker = new CuentasAnuales();
        errorMarker.setCuenta_id(cuentaId);
        errorMarker.setTransaccion("__ERROR_MARKER__:" + reason);
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