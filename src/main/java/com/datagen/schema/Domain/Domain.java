package com.datagen.schema.Domain;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by shiva on 3/2/18.
 */
public abstract class Domain {
    public enum Type{VALUE,RANGE}
    private List<String> values;
    private Range range;
    private String name;
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public Range getRange() {
        return range;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public Type getType() {
        return type;
    }

    private StringDomain.Type type;
    public void setType(Type type) {
        this.type = type;
        switch (type){
            case VALUE:
                if(this.values == null)
                values = new LinkedList<String>();
                break;
            case RANGE:
                if (this.range == null)
                range = new Range();
                break;
        }

    }

    public Domain(){}

    public Domain(StringDomain.Type type){
        switch (type){
            case VALUE:
                values = new LinkedList<String>();
                this.type = StringDomain.Type.VALUE;
                break;
            case RANGE:
                range = new Range();
                this.type = type;
                break;
        }
    }
}
