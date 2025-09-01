package com.batch.SpringBatchApp.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.batch.SpringBatchApp.entities.Transacciones;

@Service
public interface TransaccionesService {

    List<Transacciones> saveAll(Iterable<? extends Transacciones> transactionsList);

}
