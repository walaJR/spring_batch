package com.batch.SpringBatchApp.steps;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.entities.CuentasAnuales;
import com.batch.SpringBatchApp.service.CuentasAnualesService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CuentasAnualesItemWriter implements ItemWriter<CuentasAnuales> {

    @Autowired
    private CuentasAnualesService cuentasAnualesService;

    @Override
    public void write(@NonNull Chunk<? extends CuentasAnuales> chunk) throws Exception {
        log.info("=== ESCRIBIENDO CHUNK DE CUENTAS ANUALES ===");
        log.info("Número de registros de cuentas anuales a procesar: {}", chunk.size());

        if (chunk.isEmpty()) {
            log.warn("Chunk de cuentas anuales vacío recibido, no hay nada que procesar");
            return;
        }

        // Log de detalle de cada registro (solo en nivel DEBUG para no saturar logs)
        if (log.isDebugEnabled()) {
            chunk.forEach(cuentaAnual -> log.debug(
                    "Registro de cuenta anual a guardar: cuenta_id={}, fecha={}, transaccion={}, monto={}, descripcion={}",
                    cuentaAnual.getCuenta_id(), cuentaAnual.getFecha(), cuentaAnual.getTransaccion(),
                    cuentaAnual.getMonto(),
                    cuentaAnual.getDescripcion().substring(0, Math.min(50, cuentaAnual.getDescripcion().length()))
                            + "..."));
        }

        try {
            // Guardar el chunk completo
            cuentasAnualesService.saveAll(chunk.getItems());

            log.info("✓ Chunk de {} registros de cuentas anuales guardado exitosamente en la base de datos",
                    chunk.size());

            // Log de resumen de cuenta_ids guardados
            StringBuilder idsGuardados = new StringBuilder();
            chunk.forEach(cuentaAnual -> idsGuardados.append(cuentaAnual.getCuenta_id()).append(", "));
            if (idsGuardados.length() > 0) {
                idsGuardados.setLength(idsGuardados.length() - 2); // Quitar la última coma
                log.info("cuenta_ids guardados: [{}]", idsGuardados.toString());
            }

        } catch (Exception e) {
            log.error("✗ Error crítico al guardar el chunk de {} registros de cuentas anuales", chunk.size());
            log.error("Detalles del error: {}", e.getMessage());

            // Log adicional de los registros que fallaron
            if (log.isDebugEnabled()) {
                log.debug("Registros de cuentas anuales que fallaron al guardar:");
                chunk.forEach(cuentaAnual -> log.debug("  - cuenta_id: {}, fecha: {}, transaccion: {}, monto: {}",
                        cuentaAnual.getCuenta_id(), cuentaAnual.getFecha(),
                        cuentaAnual.getTransaccion(), cuentaAnual.getMonto()));
            }

            throw e; // Re-lanzar para que Spring Batch maneje el error
        }
    }
}