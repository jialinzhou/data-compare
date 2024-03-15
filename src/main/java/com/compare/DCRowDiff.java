package com.compare;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DCRowDiff {

    private DCRowState state;
    private Map<String, Object> columnNameValueMapLeft = new HashMap<>();
    private Map<String, Object> columnNameValueMapRight = new HashMap<>();
    private Set<String> diffColumnNameSetLeft = new HashSet<>();
    private Set<String> diffColumnNameSetRight = new HashSet<>();
}
