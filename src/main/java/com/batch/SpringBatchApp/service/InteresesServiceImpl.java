package com.batch.SpringBatchApp.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.batch.SpringBatchApp.entities.Intereses;
import com.batch.SpringBatchApp.repository.InteresesRepository;

@Service
public class InteresesServiceImpl implements InteresesService {

    @Autowired
    InteresesRepository interesesRepository;

    @Override
    public List<Intereses> saveAll(Iterable<? extends Intereses> interesesList) {
        return (List<Intereses>) interesesRepository.saveAll(interesesList);
    }

}