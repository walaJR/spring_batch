package com.batch.SpringBatchApp.entities;

import java.math.BigDecimal;
import java.time.LocalDate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "cuentas_anuales")
public class CuentasAnuales {

    @Id
    private Long cuenta_id;

    @Column(name = "fecha", nullable = false)
    private LocalDate fecha;

    @Column(name = "transaccion", nullable = false)
    private String transaccion;

    @Column(name = "monto", nullable = false)
    private BigDecimal monto;

    @Column(name = "descripcion", nullable = false)
    private String descripcion;

}
