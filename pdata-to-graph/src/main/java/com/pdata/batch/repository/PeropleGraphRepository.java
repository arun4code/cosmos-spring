package com.pdata.batch.repository;

import org.springframework.stereotype.Repository;

import com.microsoft.spring.data.gremlin.repository.GremlinRepository;
import com.pdata.batch.graph.model.PeopleGraph;


@Repository
public interface PeropleGraphRepository extends GremlinRepository<PeopleGraph, String> {
	
}

