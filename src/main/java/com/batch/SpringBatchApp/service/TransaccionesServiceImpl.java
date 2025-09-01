package com.batch.SpringBatchApp.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.batch.SpringBatchApp.entities.Transacciones;
import com.batch.SpringBatchApp.repository.TransaccionesRepository;

@Service
public class TransaccionesServiceImpl implements TransaccionesService {

    @Autowired
    private TransaccionesRepository transaccionesRepository;

    @Override
    public List<Transacciones> saveAll(Iterable<? extends Transacciones> transactionsList) {
        return (List<Transacciones>) transaccionesRepository.saveAll(transactionsList);
    }

}