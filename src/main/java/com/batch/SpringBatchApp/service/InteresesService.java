package com.batch.SpringBatchApp.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.batch.SpringBatchApp.entities.Intereses;

@Service
public interface InteresesService {

    List<Intereses> saveAll(Iterable<? extends Intereses> interesesList);

}
