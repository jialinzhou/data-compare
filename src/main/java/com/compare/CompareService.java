package com.compare;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.compare.config.DCCompareTaskSettings;
import com.compare.config.EncryptFieldConfig;
import com.compare.config.JdbcConfig;
import com.compare.task.CompareTask;
import com.compare.task.ReaderTask;
import com.compare.utils.ExcelExportUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class CompareService {

    private static String filePath = "/root/";

    /**
     * 批量读取数据
     */
    private static final int BATCH_READ_SIZE = 10000;
    /**
     * 批量读取比较key大小
     */
    private static final int BATCH_SIZE_KEY = 1000000;
    private DCCompareTaskSettings dcCompareTaskSettings;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(CompareService.class);

    private static ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 10, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100000), new ThreadFactory() {
        private AtomicInteger poolNumber = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("readerTask-" + poolNumber.getAndIncrement() + "-thread-");
            return thread;
        }
    });
    private static ThreadPoolExecutor executorCompare = new ThreadPoolExecutor(1, 10, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100000), new ThreadFactory() {
        private AtomicInteger poolNumber = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("compareTask-" + poolNumber.getAndIncrement() + "-thread-");
            return thread;
        }
    });

    private static ThreadPoolExecutor executorExport = new ThreadPoolExecutor(0, 3, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100000), new ThreadFactory() {
        private AtomicInteger poolNumber = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("exportTask-" + poolNumber.getAndIncrement() + "-thread-");
            return thread;
        }
    });
    public static void main(String[] args) throws InterruptedException {
        new CompareService().submitTaskByFile("request.json");
    }

    /**
     * 接收文件地址格式的比较任务配置
     * @param configPath
     * @throws InterruptedException
     */
    public void submitTaskByFile(String configPath) {
        try {
            URL url = CompareService.class.getClassLoader().getResource(configPath);
            DCCompareTaskSettings taskSettings = objectMapper.readValue(new File(url.getFile()), DCCompareTaskSettings.class);
            submitTask(taskSettings);
        } catch (Exception e) {
            LOGGER.error("接收文件地址格式的比较任务配置失败", e);
        }
    }

    /**
     * 接收json格式的比较任务配置
     * @param configJson
     * @throws InterruptedException
     */
    public void submitTaskByJson(String configJson) {
        try {
            DCCompareTaskSettings taskSettings = objectMapper.readValue(configJson, DCCompareTaskSettings.class);
            submitTask(taskSettings);
        } catch (Exception e) {
            LOGGER.error("接收json格式的比较任务配置失败", e);
        }
    }
    public void submitTaskList(List<DCCompareTaskSettings> taskSettingsList) {
        for (DCCompareTaskSettings taskSettings : taskSettingsList){
            submitTask(taskSettings);
        }

    }
    public void submitTask(DCCompareTaskSettings taskSettings) {
        try {
            LOGGER.info("比较任务配置信息： " + JSONUtil.toJsonStr(taskSettings));

            List<Object> sourceMap = readAllTargetColumnData(taskSettings.getSource(), taskSettings.getCompareKeys().get(0));
            List<Object> targetMap = readAllTargetColumnData(taskSettings.getTarget(), taskSettings.getCompareKeys().get(0));
            List<Object> sameList = new ArrayList<>();
            List<Object> targetMoreList = new ArrayList<>();
            getRetainList(sourceMap, targetMap, sameList, targetMoreList);
            List<Object> remainList = getRemainList(sameList, sourceMap);

            DCSummary dcSummary = new DCSummary();
            dcSummary.setEncryptFieldConfigMapLeft(getEncryptFieldConfig(taskSettings, true));
            dcSummary.setEncryptFieldConfigMapRight(getEncryptFieldConfig(taskSettings, false));
            long startTime = System.currentTimeMillis();
            dcSummary.setSettings(taskSettings);
            batch(remainList, taskSettings, dcSummary);
            batch(sameList, taskSettings, dcSummary);
            batch(targetMoreList, taskSettings, dcSummary);
            Thread exportThread = new Thread(() -> {
                try {
                    while (true) {
                        if (dcSummary.getTotalComparedRowsCount().get() == remainList.size() + sameList.size() + targetMoreList.size()) {
                            break;
                        }
                        Thread.sleep(20);
                        LOGGER.info("等待对比结束====, 比较行数={}", dcSummary.getTotalComparedRowsCount());
                    }
                    dcSummary.setCompareTime(System.currentTimeMillis() - startTime);
                    String sheetName = taskSettings.getSource().getTableName() + "==>" + taskSettings.getTarget().getTableName();
                    String fileName = taskSettings.getTaskId() + ".xlsx";
                    String fullPath = filePath + fileName;
                    ExcelExportUtil.exportExcel(fullPath, sheetName, dcSummary.getCompareTime(), dcSummary.getDiffList(), dcSummary.getSettings().getColumnMapping());
                    String result = dcSummary.getDiffList().size() > 0 ? "0" : "1";
                } catch (Exception e) {
                    LOGGER.error("数据对比导出任务报错", e);
                }

            });

            executorExport.execute(exportThread);

        } catch (Exception e) {
            LOGGER.error("提交数据比较任务配置失败", e);
        }
    }



    /**
     * 获取两个集合中公共的元素和有差异的元素
     * @param sourceList
     * @param targetList
     * @param sameList
     * @param otherMoreList
     */
    private static void getRetainList(List<Object> sourceList, List<Object> targetList, List<Object> sameList, List<Object> otherMoreList){
        Set<Object> sameSet = new HashSet<>(sourceList.size());
        sameSet.addAll(sourceList);
        for (Object other : targetList){
            if (sameSet.add(other)){
                otherMoreList.add(other);
            }else {
                sameList.add(other);
            }
        }
    }

    private static List<Object> getRemainList(List<Object> sameList, List<Object> targetList){
        Set<Object> allSet = new HashSet<>(sameList);
        List<Object> resultList = new ArrayList<>();
        for (Object o : targetList){
            if (allSet.add(o)){
                resultList.add(o);
            }
        }
        return resultList;
    }

    private static Map<String, EncryptFieldConfig> getEncryptFieldConfig(DCCompareTaskSettings taskSettings, boolean isLeft){
        Map<String, EncryptFieldConfig> resultMap = new HashMap<>();
        JdbcConfig jdbcConfig;
        if (isLeft){
            jdbcConfig = taskSettings.getSource();
        }else {
            jdbcConfig = taskSettings.getTarget();
        }
        List<EncryptFieldConfig> encryptColumns = jdbcConfig.getEncryptColumns();
        if (CollUtil.isNotEmpty(encryptColumns)){
            for (EncryptFieldConfig encryptFieldConfig : encryptColumns){
                resultMap.put(encryptFieldConfig.getColumnName(), encryptFieldConfig);
            }
        }

        return resultMap;
    }

    public static void batch(List<Object> list, DCCompareTaskSettings taskSettings, DCSummary summary){

        JdbcConfig sourceConfig = taskSettings.getSource();
        JdbcConfig targetConfig = taskSettings.getTarget();
        // 计算需要处理的批次数量
        int batchCount = (list.size() + BATCH_READ_SIZE - 1) / BATCH_READ_SIZE;
        String compareKey = taskSettings.getCompareKeys().get(0);
        // 分批处理List
        for (int i = 0; i < batchCount; i++) {
            int startIndex = i * BATCH_READ_SIZE;
            int endIndex = Math.min(startIndex + BATCH_READ_SIZE, list.size());
            List<Object> subList = list.subList(startIndex, endIndex);
            BlockingDeque<Row> queueLeft = new LinkedBlockingDeque<>();
            BlockingDeque<Row> queueRight = new LinkedBlockingDeque<>();
            AtomicBoolean leftEnd = new AtomicBoolean(false);
            AtomicBoolean rightEnd = new AtomicBoolean(false);
            ReaderTask readerTask = new ReaderTask(queueLeft, leftEnd, sourceConfig, compareKey, subList);
            ReaderTask readerTask1 = new ReaderTask(queueRight, rightEnd, targetConfig, compareKey, subList);
            executor.submit(readerTask);
            executor.submit(readerTask1);
            CompareTask compareTask = new CompareTask(leftEnd, rightEnd, queueLeft, queueRight, taskSettings.getCompareKeys(), summary);
            executorCompare.submit(compareTask);
        }
    }


    public static List<Object> readAllTargetColumnData(JdbcConfig jdbcConfig, String compareKey) {
        List<Object> resultList = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(jdbcConfig.getUrl(), jdbcConfig.getUser(), jdbcConfig.getPassword());
             Statement stmt = conn.createStatement();) {
            int offset = 0;
            boolean hasMoreData = true;
            while (hasMoreData) {
                ResultSet rs = stmt.executeQuery(String.format("SELECT %s FROM " + jdbcConfig.getTableName() + " LIMIT " + offset + ", " + BATCH_SIZE_KEY, compareKey));

                int count = 0;
                while (rs.next()) {
                    count++;
                    resultList.add(rs.getObject(compareKey));

                }

                if (count < BATCH_SIZE_KEY) {
                    hasMoreData = false;
                }

                offset += BATCH_SIZE_KEY;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }


}
