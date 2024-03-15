package com.compare.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JdbcConfig {

    /**
     * 数据库用户名
     */
    private String user;

    /**
     * 数据库密码
     */
    private String password;

    /**
     * 数据库连接地址
     */
    private String url;

    /**
     * 要比较的表名
     */
    private String tableName;

    /**
     * 待比较的加密字段配置
     */
    private List<EncryptFieldConfig> encryptColumns;
}
