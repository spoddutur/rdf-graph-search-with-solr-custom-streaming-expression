package com.solr.custom.streaming.model;

import org.apache.solr.client.solrj.io.Tuple;

public class TupleRecord extends Tuple {

    public TupleRecord() {}

    public TupleRecord(Tuple t) {
        this.fields = t.fields;
        this.EOF = t.EOF;
        this.EXCEPTION = t.EXCEPTION;
    }

    @Override
    public String toString() {
        return this.fields.toString();
    }

    @Override
    public boolean equals(Object obj) {

        if(obj instanceof Tuple) {
            Tuple t = (Tuple) obj;
            return this.fields.equals(t.fields) && this.EOF == t.EOF;
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Boolean.hashCode(this.EOF);
        result = prime * result + ((this.fields == null) ? 0 : this.fields.hashCode());
        return result;
    }

    public Tuple toTuple() {
        Tuple t = new Tuple();
        t.fields = this.fields;
        t.EOF = this.EOF;
        t.EXCEPTION = this.EXCEPTION;
        return t;
    }
}
