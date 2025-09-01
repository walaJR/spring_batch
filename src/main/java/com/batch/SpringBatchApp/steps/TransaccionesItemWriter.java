package com.batch.SpringBatchApp.steps;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.batch.SpringBatchApp.entities.Transacciones;
import com.batch.SpringBatchApp.service.TransaccionesService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TransaccionesItemWriter implements ItemWriter<Transacciones> {

    @Autowired
    private TransaccionesService transaccionesService;

    @Override
    public void write(@NonNull Chunk<? extends Transacciones> chunk) throws Exception {
        log.info("=== ESCRIBIENDO CHUNK ===");
        log.info("Número de transacciones a procesar: {}", chunk.size());

        if (chunk.isEmpty()) {
            log.warn("Chunk vacío recibido, no hay nada que procesar");
            return;
        }

        // Log de detalle de cada transacción (solo en nivel DEBUG para no saturar logs)
        if (log.isDebugEnabled()) {
            chunk.forEach(transaccion -> log.debug("Transacción a guardar: ID={}, Fecha={}, Monto={}, Tipo={}",
                    transaccion.getId(), transaccion.getFecha(),
                    transaccion.getMonto(), transaccion.getTipo()));
        }

        try {
            // Guardar el chunk completo
            transaccionesService.saveAll(chunk.getItems());

            log.info("✓ Chunk de {} transacciones guardado exitosamente en la base de datos", chunk.size());

            // Log de resumen de IDs guardados
            StringBuilder idsGuardados = new StringBuilder();
            chunk.forEach(t -> idsGuardados.append(t.getId()).append(", "));
            if (idsGuardados.length() > 0) {
                idsGuardados.setLength(idsGuardados.length() - 2); // Quitar la última coma
                log.info("IDs guardados: [{}]", idsGuardados.toString());
            }

        } catch (Exception e) {
            log.error("✗ Error crítico al guardar el chunk de {} transacciones", chunk.size());
            log.error("Detalles del error: {}", e.getMessage());

            // Log adicional de las transacciones que fallaron
            if (log.isDebugEnabled()) {
                log.debug("Transacciones que fallaron al guardar:");
                chunk.forEach(transaccion -> log.debug("  - ID: {}, Monto: {}, Tipo: {}",
                        transaccion.getId(), transaccion.getMonto(), transaccion.getTipo()));
            }

            throw e; // Re-lanzar para que Spring Batch maneje el error
        }
    }
}