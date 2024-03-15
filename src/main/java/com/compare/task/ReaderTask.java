package com.compare.task;

import cn.hutool.core.collection.CollUtil;
import com.compare.Row;
import com.compare.config.EncryptFieldConfig;
import com.compare.config.JdbcConfig;
import com.compare.utils.EncryptUtils;
import lombok.Data;

import java.sql.*;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 数据读取任务线程
 * 负责从数据库读取需要对比的数据
 */
@Data
public class ReaderTask implements Runnable{

    /**
     * 读取到的数据存放到阻塞队列中，用于比较任务线程拉取进行比较
     */
    private BlockingDeque<Row> queue;

    /**
     * 用来标识数据读取是否完成
     */
    private AtomicBoolean endFlag;

    /**
     * 存放当前数据读取任务的相关数据库配置信息
     */
    private JdbcConfig jdbcConfig;

    private List<Object> conditionList;

    private String compareKey;

    public ReaderTask(BlockingDeque<Row> queue, AtomicBoolean endFlag, JdbcConfig jdbcConfig, String compareKey, List<Object> conditionList) {
        this.queue = queue;
        this.endFlag = endFlag;
        this.jdbcConfig = jdbcConfig;
        this.compareKey = compareKey;
        this.conditionList = conditionList;
    }

    @Override
    public void run() {
        String url = jdbcConfig.getUrl();
        String user = jdbcConfig.getUser();
        String password = jdbcConfig.getPassword();
        String tableName = jdbcConfig.getTableName();

        readDataToQueue(queue, endFlag, url, user, password, tableName, compareKey, conditionList);
    }


    public void readDataToQueue(BlockingDeque<Row> queue, AtomicBoolean endFlag, String url, String user, String password, String tableName, String compareKey, List<Object> condition) {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();) {
            String whereCondition = getWhereCondition(compareKey, condition);
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + whereCondition);
            while (rs.next()) {
                Row row = new Row();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    row.getColumnNameAndValueMap().put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    fillExtraEncryptColmn(rs.getMetaData().getColumnName(i), rs.getObject(i), row);
                }
                queue.put(row);
            }
            endFlag.set(true);

        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 直接在查询时填充加密字段值，用于后续比较
     * @param columnName
     * @param originValue
     * @param row
     */
    public void fillExtraEncryptColmn(String columnName, Object originValue, Row row){
        List<EncryptFieldConfig> encryptColumns = jdbcConfig.getEncryptColumns();
        if (CollUtil.isNotEmpty(encryptColumns)){
            for (EncryptFieldConfig encryptFieldConfig : encryptColumns){
                if (columnName.equals(encryptFieldConfig.getColumnName())){
                    String encryptValue = EncryptUtils.getEncryptValue(originValue, encryptFieldConfig);
                    row.getColumnNameAndValueMap().put(encryptFieldConfig.getTargetColumnName(), encryptValue);
                }

            }
        }
    }

    public String getWhereCondition(String compareKey, List<Object> condition){
        StringBuilder whereSql = new StringBuilder(" where ");
        whereSql.append(compareKey).append(" in (").append(getJoinString(",", condition)).append(")");
        return whereSql.toString();
    }

    public String getJoinString(CharSequence delimiter, List<Object> condition){
        StringJoiner joiner = new StringJoiner(delimiter);
        for (Object cs: condition) {
            //判断是否是数字，数字不需要加引号
            if (condition instanceof Number){
                joiner.add(cs.toString());
            }else {
                joiner.add("'" + cs.toString() + "'");
            }
        }
        return joiner.toString();
    }
}
