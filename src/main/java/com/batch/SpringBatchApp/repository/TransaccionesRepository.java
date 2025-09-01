package com.batch.SpringBatchApp.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.batch.SpringBatchApp.entities.Transacciones;

@Repository
public interface TransaccionesRepository extends JpaRepository<Transacciones, Long> {

}