package io.clubone.billing.service.report;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CSV and XLSX export with stable key order.
 * XLSX uses streaming {@link SXSSFWorkbook} (windowed rows) to avoid OOM on large exports.
 */
public final class ReportExportHelper {

    /** Keep only this many rows in memory while writing XLSX. */
    private static final int SXSSF_ROW_WINDOW = 100;

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
        StringBuilder sb = new StringBuilder(Math.min(rows.size() * 64 + 256, 4 * 1024 * 1024));
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
        try (SXSSFWorkbook wb = new SXSSFWorkbook(SXSSF_ROW_WINDOW);
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            wb.setCompressTempFiles(true);
            String safeName = sheetName == null ? "Report" : sheetName.substring(0, Math.min(31, sheetName.length()));
            Sheet sh = wb.createSheet(safeName);
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
            wb.dispose();
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
