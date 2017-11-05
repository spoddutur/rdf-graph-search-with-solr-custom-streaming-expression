package com.solr.custom.streaming.model;

import java.util.ArrayList;
import java.util.List;

public
class Paths {
    private List<Path> paths = new ArrayList<Path>();

    public List<Path> getPaths() {
        return this.paths;
    }

    public static Paths getInstance(Path p) {
        Paths paths = new Paths();
        paths.addPath(p);
        return paths;
    }

    public void addPath(Path p) {
        paths.add(p);
    }

    @Override
    public String toString() {
        return this.paths.toString();
    }
}
