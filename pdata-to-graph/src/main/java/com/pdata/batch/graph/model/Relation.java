package com.pdata.batch.graph.model;

import org.springframework.data.annotation.Id;

import com.microsoft.spring.data.gremlin.annotation.Edge;
import com.microsoft.spring.data.gremlin.annotation.EdgeFrom;
import com.microsoft.spring.data.gremlin.annotation.EdgeTo;

import lombok.Data;

@Edge
@Data
public class Relation {

    @Id
    private String id;

    private String name;

    @EdgeFrom
    private PeopleGraph2 personFrom;

    @EdgeTo
    private PeopleGraph2 personTo;

}