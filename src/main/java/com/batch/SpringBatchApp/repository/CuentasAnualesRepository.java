package com.batch.SpringBatchApp.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.batch.SpringBatchApp.entities.CuentasAnuales;

@Repository
public interface CuentasAnualesRepository extends JpaRepository<CuentasAnuales, Long> {

}
