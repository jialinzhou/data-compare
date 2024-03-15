package com.compare;


import com.compare.config.DCCompareTaskSettings;
import com.compare.config.EncryptFieldConfig;
import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Data
@ToString
public class DCSummary {
    private DCCompareTaskSettings settings;
    private List<DCRowDiff> diffList = Collections.synchronizedList(new ArrayList<>());
    private AtomicLong totalComparedRowsCount = new AtomicLong(0);
    private AtomicLong insertedRowsCount = new AtomicLong(0);
    private AtomicLong deletedRowsCount = new AtomicLong(0);
    private AtomicLong modifiedRowsCount = new AtomicLong(0);
    private long compareTime;
    private boolean success;

    private Map<String, EncryptFieldConfig> encryptFieldConfigMapLeft;
    private Map<String, EncryptFieldConfig> encryptFieldConfigMapRight;
}
