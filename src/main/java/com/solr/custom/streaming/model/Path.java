package com.solr.custom.streaming.model;

import org.apache.solr.client.solrj.io.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Path implements  Cloneable {
    private List<Tuple> nodes = new ArrayList<Tuple>();

    public List<Tuple> getNodes() {
        return this.nodes;
    }

    public static Path getInstance(Collection<Tuple> c) {
        Path p = new Path();
        p.nodes.addAll(c);
        return p;
    }

    public Path add(Tuple t) {
        nodes.add(t);
        return this;
    }

    public Path clone() {
        Path p = new Path();
        p.nodes.addAll(this.nodes);
        return p;
    }

    @Override
    public String toString() {
        return this.nodes.toString();
    }
}
