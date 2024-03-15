package com.compare;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * 用来存放从数据库表中查询的一行数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Row {

    private Map<String, Object> columnNameAndValueMap = new HashMap<>();


}
