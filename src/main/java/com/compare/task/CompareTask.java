package com.compare.task;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.compare.DCRowDiff;
import com.compare.DCRowState;
import com.compare.DCSummary;
import com.compare.Row;
import com.compare.config.DCCompareTaskSettings;
import com.compare.config.EncryptFieldConfig;
import com.compare.utils.DBUtils;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 数据对比任务线程
 */
@Data
public class CompareTask implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(CompareTask.class);

    private DCSummary summary;

    private AtomicBoolean leftEnd;
    private AtomicBoolean rightEnd;

    private BlockingDeque<Row> queueLeft;

    private BlockingDeque<Row> queueRight;

    private List<String> compareKeys;



    public CompareTask(AtomicBoolean leftEnd, AtomicBoolean rightEnd, BlockingDeque<Row> queueLeft, BlockingDeque<Row> queueRight,
                       List<String> compareKeys, DCSummary dcSummary) {
        this.leftEnd = leftEnd;
        this.rightEnd = rightEnd;
        this.queueLeft = queueLeft;
        this.queueRight = queueRight;
        this.compareKeys = compareKeys;
        this.summary = dcSummary;
    }

    @Override
    public void run() {
        try {
            compareDataFromQueue(queueLeft, queueRight, compareKeys);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void compareDataFromQueue(BlockingDeque<Row> queueLeft, BlockingDeque<Row> queueRight, List<String> compareKeys) throws InterruptedException {

        while (true){
            if (leftEnd.get() && rightEnd.get()){
                break;
            }
        }
        while (!queueLeft.isEmpty() && !queueRight.isEmpty()) {
            this.summary.getTotalComparedRowsCount().getAndIncrement();
            Row rowLeft = queueLeft.take();
            Row rowRight = queueRight.take();

            int keys = compareKeys(rowLeft, rowRight, compareKeys);

            if (keys != 0) {
                if (keys < 0) {
                    queueRight.push(rowRight);
                    queueLeft.put(rowLeft);
                } else {
                    queueLeft.push(rowLeft);
                    queueRight.put(rowRight);
                }
            } else {
                compareRows(rowLeft.getColumnNameAndValueMap(), rowRight.getColumnNameAndValueMap());
            }

        }

        while(!queueLeft.isEmpty() && queueRight.isEmpty() && rightEnd.get()) {
            this.summary.getTotalComparedRowsCount().getAndIncrement();
            Row rowLeft = queueLeft.poll();
            this.summary.getInsertedRowsCount().getAndIncrement();
            DCRowDiff dcRowDiff = new DCRowDiff();
            dcRowDiff.setState(DCRowState.INSERTED);
            dcRowDiff.setColumnNameValueMapLeft(rowLeft.getColumnNameAndValueMap());
            this.summary.getDiffList().add(dcRowDiff);
        }

        while(!queueRight.isEmpty() && queueLeft.isEmpty() && leftEnd.get()) {
            this.summary.getTotalComparedRowsCount().getAndIncrement();
            Row rowRight = queueRight.poll();
            this.summary.getDeletedRowsCount().getAndIncrement();
            DCRowDiff dcRowDiff = new DCRowDiff();
            dcRowDiff.setState(DCRowState.DELETED);
            dcRowDiff.setColumnNameValueMapRight(rowRight.getColumnNameAndValueMap());
            this.summary.getDiffList().add(dcRowDiff);
        }



    }

    /**
     * 根据给定的比较字段比较行数据
     * @param left
     * @param right
     * @param compareKeys
     * @return
     */
    public int compareKeys(Row left, Row right, List<String> compareKeys) {
        for(int index = 0; index < compareKeys.size(); ++index) {
            Object value = left.getColumnNameAndValueMap().get(compareKeys.get(index));
            Object otherValue = right.getColumnNameAndValueMap().get(compareKeys.get(index));
            LOGGER.info("当前比较信息:{}={} --> {}={}", compareKeys.get(index), value, compareKeys.get(index), otherValue);
            int result = DBUtils.compareDataValues(value, otherValue);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    /**
     * 对给定的两行数据进行对比
     * @param rowLeft
     * @param rowRight
     */
    public void compareRows(Map<String, Object> rowLeft, Map<String, Object> rowRight) {
        boolean same = true;
        Set<String> diffColumnNameSetLeft = new HashSet<>();
        Set<String> diffColumnNameSetRight = new HashSet<>();
        for (String key : summary.getSettings().getColumnMapping().keySet()) {
            Object leftValue = rowLeft.get(key);
            Object rightValue = rowRight.get(summary.getSettings().getColumnMapping().get(key));
            int i = DBUtils.compareDataValues(leftValue, rightValue);
            if (i != 0) {
                same = false;
                diffColumnNameSetLeft.add(key);
                diffColumnNameSetRight.add(key);
                String mappingNameLeft = getMappingName(key, true);
                String mappingNameRight = getMappingName(key, false);
                if (StrUtil.isNotEmpty(mappingNameLeft)){
                    diffColumnNameSetLeft.add(mappingNameLeft);
                }
                if (StrUtil.isNotEmpty(mappingNameRight)){
                    diffColumnNameSetRight.add(mappingNameRight);
                }
            }
        }
        if (!same){
            this.summary.getModifiedRowsCount().getAndIncrement();
            DCRowDiff dcRowDiff = new DCRowDiff();
            dcRowDiff.setDiffColumnNameSetLeft(diffColumnNameSetLeft);
            dcRowDiff.setDiffColumnNameSetRight(diffColumnNameSetRight);
            dcRowDiff.setState(DCRowState.MODIFIED);
            dcRowDiff.setColumnNameValueMapRight(rowRight);
            dcRowDiff.setColumnNameValueMapLeft(rowLeft);
            this.summary.getDiffList().add(dcRowDiff);

        }
    }
    private String getMappingName(String cloumnName, boolean isLeft){
        DCCompareTaskSettings settings = summary.getSettings();
        List<EncryptFieldConfig> encryptColumns;
        if (isLeft){
            encryptColumns = settings.getSource().getEncryptColumns();
        }else {
            encryptColumns = settings.getTarget().getEncryptColumns();
        }
        if (CollUtil.isNotEmpty(encryptColumns)){
            for (EncryptFieldConfig encryptFieldConfig : encryptColumns){
                if (cloumnName.equals(encryptFieldConfig.getTargetColumnName())){
                    return encryptFieldConfig.getColumnName();
                }
            }
        }
        return null;
    }
}
