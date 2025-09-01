package com.batch.SpringBatchApp.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.batch.SpringBatchApp.entities.CuentasAnuales;

@Service
public interface CuentasAnualesService {

    List<CuentasAnuales> saveAll(Iterable<? extends CuentasAnuales> cuentasAnualesList);

}
