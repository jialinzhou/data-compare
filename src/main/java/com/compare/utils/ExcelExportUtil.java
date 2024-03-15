package com.compare.utils;

import cn.hutool.core.io.FileUtil;
import com.compare.DCRowDiff;
import com.compare.DCRowState;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ExcelExportUtil {

    public static void exportExcel(String filePath, List<String> headers, List<List<Object>> data, List<int[]> colorCells) {
        try (Workbook workbook = new XSSFWorkbook(); FileOutputStream fileOut = new FileOutputStream(filePath)) {
            Sheet sheet = workbook.createSheet("Sheet1");

            // 设置表头
            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < headers.size(); i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers.get(i));
            }

            // 设置数据
            for (int i = 0; i < data.size(); i++) {
                Row row = sheet.createRow(i + 1);
                List<Object> rowData = data.get(i);
                for (int j = 0; j < rowData.size(); j++) {
                    Cell cell = row.createCell(j);
                    cell.setCellValue(String.valueOf(rowData.get(j)));
                }
            }

            // 设置指定单元格颜色
            CellStyle cellStyle = workbook.createCellStyle();
            cellStyle.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
            cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            for (int[] colorCell : colorCells) {
                Row row = sheet.getRow(colorCell[0]);
                Cell cell = row.getCell(colorCell[1]);
                cell.setCellStyle(cellStyle);
            }

            workbook.write(fileOut);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void exportExcel(String filePath, String sheetName, long costTime, List<DCRowDiff> data, Map<String, String> columnNameMapping) {
        FileUtil.mkParentDirs(filePath);
        try (Workbook workbook = new XSSFWorkbook(); FileOutputStream fileOut = new FileOutputStream(filePath)) {
            CellStyle cellStyleBlue = workbook.createCellStyle();
            cellStyleBlue.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
            cellStyleBlue.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            CellStyle cellStylePink = workbook.createCellStyle();
            cellStylePink.setFillForegroundColor(IndexedColors.PINK.getIndex());
            cellStylePink.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            CellStyle cellStyleGreen = workbook.createCellStyle();
            cellStyleGreen.setFillForegroundColor(IndexedColors.LIGHT_GREEN.getIndex());
            cellStyleGreen.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            CellStyle cellStyleYellow = workbook.createCellStyle();
            cellStyleYellow.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
            cellStyleYellow.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            CellStyle cellStyleRed = workbook.createCellStyle();
            cellStyleRed.setFillForegroundColor(IndexedColors.RED.getIndex());
            cellStyleRed.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            Sheet sheet = workbook.createSheet(sheetName);
            List<String> tableLeftColumnList = columnNameMapping.keySet().stream().collect(Collectors.toList());
            List<String> headers = new ArrayList<>();
            List<String> tableRightColumnList = new ArrayList<>();
            for (String columnName : tableLeftColumnList){
                tableRightColumnList.add(columnNameMapping.get(columnName));
            }
            headers.addAll(tableLeftColumnList);
            headers.addAll(tableRightColumnList);
            // 设置表头
            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < tableLeftColumnList.size(); i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers.get(i));
                cell.setCellStyle(cellStyleBlue);
            }
            for (int i = 0; i < tableRightColumnList.size(); i++) {
                Cell cell = headerRow.createCell(i + tableLeftColumnList.size());
                cell.setCellValue(headers.get(i));
                cell.setCellStyle(cellStylePink);
            }
            //增加耗时展示
            Cell cellCost = headerRow.createCell(tableLeftColumnList.size() + tableRightColumnList.size() + 1);
            cellCost.setCellValue("耗时=" + costTime + "毫秒");
            // 设置数据
            for (int i = 0; i < data.size(); i++) {
                DCRowDiff rowData = data.get(i);
                if (Objects.isNull(rowData)){
                    continue;
                }
                Row row = sheet.createRow(i + 1);
                Set<String> diffColumnNameSetLeft = rowData.getDiffColumnNameSetLeft();
                Set<String> diffColumnNameSetRight = rowData.getDiffColumnNameSetRight();
                if (DCRowState.INSERTED.equals(rowData.getState())){
                    Map<String, Object> valueMapLeft = rowData.getColumnNameValueMapLeft();
                    for (int j = 0; j < tableLeftColumnList.size(); j++) {
                        Cell cell = row.createCell(j);
                        cell.setCellValue(String.valueOf(valueMapLeft.get(tableLeftColumnList.get(j))));
                        cell.setCellStyle(cellStyleGreen);
                    }
                }else if (DCRowState.DELETED.equals(rowData.getState())){
                    Map<String, Object> valueMapRight = rowData.getColumnNameValueMapRight();
                    for (int j = 0; j < tableRightColumnList.size(); j++) {
                        Cell cell = row.createCell(j + tableLeftColumnList.size());
                        cell.setCellValue(String.valueOf(valueMapRight.get(tableRightColumnList.get(j))));
                        cell.setCellStyle(cellStyleRed);
                    }
                }else{
                    Map<String, Object> valueMapLeft = rowData.getColumnNameValueMapLeft();
                    for (int j = 0; j < tableLeftColumnList.size(); j++) {
                        Cell cell = row.createCell(j);
                        cell.setCellValue(String.valueOf(valueMapLeft.get(tableLeftColumnList.get(j))));
                        if (diffColumnNameSetLeft.contains(tableLeftColumnList.get(j))){
                            cell.setCellStyle(cellStyleYellow);
                        }
                    }

                    Map<String, Object> valueMapRight = rowData.getColumnNameValueMapRight();
                    for (int j = 0; j < tableRightColumnList.size(); j++) {
                        Cell cell = row.createCell(j + tableLeftColumnList.size());
                        cell.setCellValue(String.valueOf(valueMapRight.get(tableRightColumnList.get(j))));
                        if (diffColumnNameSetRight.contains(tableLeftColumnList.get(j))){
                            cell.setCellStyle(cellStyleYellow);
                        }
                    }
                }

            }


            workbook.write(fileOut);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        List<String> headers = new ArrayList<>();
        headers.add("姓名");
        headers.add("年龄");
        headers.add("性别");
        List<List<Object>> data = new ArrayList<>();
        List<Object> data1 = new ArrayList<>();
        data1.add("Alice");
        data1.add(25);
        data1.add("Female");
        List<Object> data2 = new ArrayList<>();
        data2.add("Bob");
        data2.add(30);
        data2.add("Male");
        data.add(data1);
        data.add(data2);
        List<int[]> colorCells = new ArrayList<>();// 设置第二行第二列单元格颜色
        colorCells.add(new int[]{1,1});
        colorCells.add(new int[]{1,1});

        exportExcel("output.xlsx", headers, data, colorCells);
    }
}
