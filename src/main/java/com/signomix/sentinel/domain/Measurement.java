package com.signomix.sentinel.domain;

import java.sql.Timestamp;
import java.util.HashMap;

public class Measurement {
    public Timestamp timestamp;
    public HashMap<String, Double> values;
}
