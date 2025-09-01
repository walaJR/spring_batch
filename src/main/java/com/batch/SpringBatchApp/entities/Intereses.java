package com.batch.SpringBatchApp.entities;

import java.math.BigDecimal;

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
@Table(name = "intereses")
public class Intereses {

    @Id
    private Long cuenta_id;

    @Column(name = "nombre", nullable = false)
    private String nombre;

    @Column(name = "saldo", nullable = false)
    private BigDecimal saldo;

    @Column(name = "edad", nullable = false)
    private int edad;

    @Column(name = "tipo", nullable = false)
    private String tipo;

}
