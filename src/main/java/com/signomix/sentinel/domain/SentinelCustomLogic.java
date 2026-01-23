package com.signomix.sentinel.domain;

import java.util.HashMap;
import java.util.List;

import com.signomix.common.iot.sentinel.SentinelConfig;

import io.agroal.api.AgroalDataSource;

public interface SentinelCustomLogic {

    boolean hasMultipleMeasurements();
    ConditionResult run(SentinelConfig config, String[] messageArray, int deviceRuleStatus,
            HashMap<String, String> scriptProperties,
            AgroalDataSource olapDs,
            List<Measurement> measurements
    );

}
