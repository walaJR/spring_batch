package com.batch.SpringBatchApp.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.batch.SpringBatchApp.entities.CuentasAnuales;
import com.batch.SpringBatchApp.repository.CuentasAnualesRepository;

@Service
public class CuentasAnualesServiceImpl implements CuentasAnualesService {

    @Autowired
    CuentasAnualesRepository cuentasAnualesRepository;

    @Override
    public List<CuentasAnuales> saveAll(Iterable<? extends CuentasAnuales> cuentasAnualesList) {
        return (List<CuentasAnuales>) cuentasAnualesRepository.saveAll(cuentasAnualesList);
    }

}
