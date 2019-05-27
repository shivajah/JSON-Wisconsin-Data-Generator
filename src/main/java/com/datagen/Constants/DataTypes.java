package com.datagen.Constants;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by shiva on 2/22/18.
 */
public class DataTypes {

    public enum DataType {
        STRING,
        INTEGER,
        BINARY
    }

    public static Map<String, String> toMap() {
        Map<String, String> map = new LinkedHashMap<>();
        for (DataType d : DataType.values()) {
            map.put(d.name(), d.name());
        }
        return map;
    }
}
