package com.batch.SpringBatchApp.steps;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.entities.Intereses;
import com.batch.SpringBatchApp.service.InteresesService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InteresesItemWriter implements ItemWriter<Intereses> {

    @Autowired
    private InteresesService interesesService;

    @Override
    public void write(@NonNull Chunk<? extends Intereses> chunk) throws Exception {
        log.info("=== ESCRIBIENDO CHUNK DE INTERESES ===");
        log.info("Número de registros de interés a procesar: {}", chunk.size());

        if (chunk.isEmpty()) {
            log.warn("Chunk de intereses vacío recibido, no hay nada que procesar");
            return;
        }

        // Log de detalle de cada registro (solo en nivel DEBUG para no saturar logs)
        if (log.isDebugEnabled()) {
            chunk.forEach(interes -> log.debug(
                    "Registro de interés a guardar: cuenta_id={}, nombre={}, saldo={}, edad={}, tipo={}",
                    interes.getCuenta_id(), interes.getNombre(), interes.getSaldo(),
                    interes.getEdad(), interes.getTipo()));
        }

        try {
            // Guardar el chunk completo
            interesesService.saveAll(chunk.getItems());

            log.info("✓ Chunk de {} registros de interés guardado exitosamente en la base de datos", chunk.size());

            // Log de resumen de cuenta_ids guardados
            StringBuilder idsGuardados = new StringBuilder();
            chunk.forEach(interes -> idsGuardados.append(interes.getCuenta_id()).append(", "));
            if (idsGuardados.length() > 0) {
                idsGuardados.setLength(idsGuardados.length() - 2); // Quitar la última coma
                log.info("cuenta_ids guardados: [{}]", idsGuardados.toString());
            }

        } catch (Exception e) {
            log.error("✗ Error crítico al guardar el chunk de {} registros de interés", chunk.size());
            log.error("Detalles del error: {}", e.getMessage());

            // Log adicional de los registros que fallaron
            if (log.isDebugEnabled()) {
                log.debug("Registros de interés que fallaron al guardar:");
                chunk.forEach(interes -> log.debug("  - cuenta_id: {}, nombre: {}, saldo: {}, tipo: {}",
                        interes.getCuenta_id(), interes.getNombre(), interes.getSaldo(), interes.getTipo()));
            }

            throw e; // Re-lanzar para que Spring Batch maneje el error
        }
    }
}