package com.batch.SpringBatchApp.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.batch.SpringBatchApp.entities.Intereses;

@Repository
public interface InteresesRepository extends JpaRepository<Intereses, Long> {

}
