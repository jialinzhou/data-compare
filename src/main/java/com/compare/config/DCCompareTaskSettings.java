package com.compare.config;


import lombok.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DCCompareTaskSettings {

    private Long taskId;
    /**
     * 源端数据源配置
     */
    private JdbcConfig source;

    /**
     * 目标端数据源配置
     */
    private JdbcConfig target;


    /**
     * 用作唯一记录判断的字段
     */
    private List<String> compareKeys;

    /**
     * 源端和目标端表字段映射关系
     */
    private Map<String, String> columnMapping = new HashMap<>();

}
