package com.pdata.batch.graph.model;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;

import com.microsoft.spring.data.gremlin.annotation.EdgeSet;
import com.microsoft.spring.data.gremlin.annotation.Graph;
import com.microsoft.spring.data.gremlin.annotation.VertexSet;

import lombok.Getter;
import lombok.Setter;

@Graph
@Getter
@Setter
public class Network {

    @Id
    private String id;

    public Network() {
        this.edges = new ArrayList<Object>();
        this.vertexes = new ArrayList<Object>();
    }

    @EdgeSet
    private List<Object> edges;

    @VertexSet
    private List<Object> vertexes;
    
}