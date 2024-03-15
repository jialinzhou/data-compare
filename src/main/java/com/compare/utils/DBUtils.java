package com.compare.utils;

import cn.hutool.core.util.StrUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DBUtils {
    private static final String URL = "jdbc:mysql://localhost:3306/test_target?useSSL=false&serverTimezone=UTC";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static final int PAGE_SIZE = 1000;

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String tableName = "user_info";
            Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
            BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(1000);
            getAllRecords(connection, tableName, blockingQueue);

            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void getAllRecords(Connection connection, String tableName, BlockingQueue<String> blockingQueue){
        try {
            int totalRecords = getTotalRecords(connection, tableName);
            int totalPages = (int) Math.ceil((double) totalRecords / PAGE_SIZE);

            for (int currentPage = 1; currentPage <= totalPages; currentPage++) {
                queryData(connection, currentPage, tableName, blockingQueue);
            }
        }catch (Exception e){
           e.printStackTrace();
        }

    }

    /**
     * 获取指定表的全量数据总数
     * @param connection
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static int getTotalRecords(Connection connection, String tableName) throws SQLException {
        String sql = String.format("SELECT COUNT(*) FROM %s", tableName);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        int totalRecords = 0;
        if (resultSet.next()) {
            totalRecords = resultSet.getInt(1);
        }
        resultSet.close();
        preparedStatement.close();
        return totalRecords;
    }

    private static void queryData(Connection connection, int currentPage, String tableName, BlockingQueue<String> blockingQueue) throws SQLException {
        int offset = (currentPage - 1) * PAGE_SIZE;
        String sql = String.format("SELECT * FROM %s LIMIT ? OFFSET ?", tableName);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, PAGE_SIZE);
        preparedStatement.setInt(2, offset);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            // 处理查询结果，例如打印每行数据
            blockingQueue.offer(resultSet.getString("name"));
        }

        resultSet.close();
        preparedStatement.close();
    }



    public static int compareDataValues(Object cell1, Object cell2) {
        if (cell1 == cell2 || (Objects.isNull(cell1) && Objects.isNull(cell2))) {
            return 0;
        } else if (Objects.isNull(cell1)) {
            return 1;
        } else if (Objects.isNull(cell2)) {
            return -1;
        } else if (cell1 instanceof Number && cell2 instanceof Number) {
            // Actual data type for the same column may differ (e.g. partially read from server, partially added on client side)
            return compareNumbers((Number)cell1, (Number)cell2);
        } else if (cell1 instanceof Comparable && cell1.getClass() == cell2.getClass()) {
            return ((Comparable) cell1).compareTo(cell2);
        } else {
            if (cell1 instanceof Number) {
                Object num2 = convertString(String.valueOf(cell2), cell1.getClass());
                if (num2 == null) {
                    return -1;
                }
                if (num2 instanceof Number) {
                    return compareNumbers((Number) cell1, (Number) num2);
                }
            } else if (cell2 instanceof Number) {
                Object num1 = convertString(String.valueOf(cell1), cell2.getClass());
                if (num1 == null) {
                    return 1;
                }
                if (num1 instanceof Number) {
                    return compareNumbers((Number) num1, (Number) cell2);
                }
            }
            String str1 = String.valueOf(cell1);
            String str2 = String.valueOf(cell2);
            return str1.compareTo(str2);
        }
    }

    public static int compareNumbers(Number num1, Number num2){
        if (num1.doubleValue() > num2.doubleValue()) {
            return 1; // num1 is greater than num2
        } else if (num1.doubleValue() < num2.doubleValue()) {
            return -1; // num1 is less than num2
        } else {
            return 0; // num1 is equal to num2
        }
    }

    public static Object convertString(String value, Class<?> valueType) {
        try {
            if (StrUtil.isEmpty(value)) {
                return null;
            }
            if (valueType == null || CharSequence.class.isAssignableFrom(valueType)) {
                return value;
            } else if (valueType == Boolean.class || valueType == Boolean.TYPE) {
                return Boolean.valueOf(value);
            } else if (valueType == Long.class) {
                return Long.valueOf(normalizeIntegerString(value));
            } else if (valueType == Long.TYPE) {
                return Long.parseLong(normalizeIntegerString(value));
            } else if (valueType == Integer.class) {
                return Integer.valueOf(normalizeIntegerString(value));
            } else if (valueType == Integer.TYPE) {
                return Integer.parseInt(normalizeIntegerString(value));
            } else if (valueType == Short.class) {
                return Short.valueOf(normalizeIntegerString(value));
            } else if (valueType == Short.TYPE) {
                return Short.parseShort(normalizeIntegerString(value));
            } else if (valueType == Byte.class) {
                return Byte.valueOf(normalizeIntegerString(value));
            } else if (valueType == Byte.TYPE) {
                return Byte.parseByte(normalizeIntegerString(value));
            } else if (valueType == Double.class) {
                return Double.valueOf(value);
            } else if (valueType == Double.TYPE) {
                return Double.parseDouble(value);
            } else if (valueType == Float.class) {
                return Float.valueOf(value);
            } else if (valueType == Float.TYPE) {
                return Float.parseFloat(value);
            } else if (valueType == BigInteger.class) {
                return new BigInteger(normalizeIntegerString(value));
            } else if (valueType == BigDecimal.class) {
                return new BigDecimal(value);
            } else {
                return value;
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            return value;
        }
    }

    private static String normalizeIntegerString(String value) {
        int divPos = value.lastIndexOf('.');
        return divPos == -1 ? value : value.substring(0, divPos);
    }
}

