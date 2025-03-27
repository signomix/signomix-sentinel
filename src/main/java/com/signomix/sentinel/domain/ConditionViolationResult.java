package com.signomix.sentinel.domain;

import org.python.antlr.base.boolop;

public class ConditionViolationResult {
    public String measurement;
    public Double value;
    public boolean violated;
    public String eui;
    public boolean error = false;
    public String errorMessage = "";
}
