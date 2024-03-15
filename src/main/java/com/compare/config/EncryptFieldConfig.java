package com.compare.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EncryptFieldConfig {

    /**
     * 加密字段名称
     */
    private String columnName;

    /**
     * 加密器名称
     */
    private String encryptType;
    /**
     * 加密key
     */
    private String encryptKey;

    /**
     * 加密后新增的字段名称
     */
    private String targetColumnName;

}
