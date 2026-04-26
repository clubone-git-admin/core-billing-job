package io.clubone.billing.service.report;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CSV and XLSX export with stable key order (first row keys of the first data row, or from {@code columnKeys}).
 */
public final class ReportExportHelper {

    private ReportExportHelper() {
    }

    public static String toCsv(List<String> columnKeys, List<Map<String, Object>> rows) {
        List<String> keys =
                (columnKeys != null && !columnKeys.isEmpty())
                        ? columnKeys
                        : (rows.isEmpty() ? List.of() : new ArrayList<>(rows.get(0).keySet()));
        if (keys.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(
                keys.stream()
                        .map(ReportExportHelper::escapeCsv)
                        .collect(Collectors.joining(",")));
        sb.append('\n');
        for (Map<String, Object> r : rows) {
            String line =
                    keys.stream()
                            .map(
                                    k -> {
                                        Object v = r.get(k);
                                        return escapeCsv(
                                                v == null ? "" : String.valueOf(v));
                                    })
                            .collect(Collectors.joining(","));
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

    public static byte[] toXlsx(String sheetName, List<String> columnKeys, List<Map<String, Object>> rows)
            throws IOException {
        List<String> keys =
                (columnKeys != null && !columnKeys.isEmpty())
                        ? columnKeys
                        : (rows.isEmpty() ? List.of() : new ArrayList<>(rows.get(0).keySet()));
        try (XSSFWorkbook wb = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Sheet sh = wb.createSheet(sheetName == null ? "Report" : sheetName.substring(0, Math.min(31, sheetName.length())));
            int r = 0;
            Row head = sh.createRow(r++);
            for (int i = 0; i < keys.size(); i++) {
                head.createCell(i).setCellValue(keys.get(i));
            }
            for (Map<String, Object> row : rows) {
                Row x = sh.createRow(r++);
                for (int i = 0; i < keys.size(); i++) {
                    Object v = row.get(keys.get(i));
                    if (v == null) {
                        x.createCell(i);
                    } else if (v instanceof Number n) {
                        x.createCell(i).setCellValue(n.doubleValue());
                    } else {
                        x.createCell(i).setCellValue(String.valueOf(v));
                    }
                }
            }
            wb.write(out);
            return out.toByteArray();
        }
    }

    public static String utf8BomString(String s) {
        if (s == null) {
            return null;
        }
        return new String(
                new byte[] {(byte) 0xef, (byte) 0xbb, (byte) 0xbf},
                StandardCharsets.UTF_8) + s;
    }

    private static String escapeCsv(String str) {
        if (str == null) {
            return "";
        }
        boolean need =
                str.contains(",")
                        || str.contains("\n")
                        || str.contains("\r")
                        || str.contains("\"");
        String t = str.replace("\"", "\"\"");
        if (need) {
            return "\"" + t + "\"";
        }
        return t;
    }
}
