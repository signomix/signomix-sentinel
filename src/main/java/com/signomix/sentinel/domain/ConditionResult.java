package com.signomix.sentinel.domain;

public class ConditionResult {
    public String measurement=null;
    public Double value=null;
    public boolean violated=false;
    public String eui=null;
    public String condition=null;
    public boolean error = false;
    public String errorMessage = "";
    public String commandTarget = null;
    public String command = null;
    public Long configId = null;
    //public String messageId = null;
}
