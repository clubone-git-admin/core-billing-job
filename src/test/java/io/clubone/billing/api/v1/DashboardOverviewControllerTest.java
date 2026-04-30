package io.clubone.billing.api.v1;

import io.clubone.billing.service.DashboardService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DashboardController.class)
class DashboardOverviewControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DashboardService dashboardService;

    @Test
    void overviewReturnsStableContractWithRunHealthDefaults() throws Exception {
        Map<String, Object> runHealthItem = new LinkedHashMap<>();
        runHealthItem.put("billing_run_id", null);
        runHealthItem.put("status", null);
        runHealthItem.put("due_date", null);
        runHealthItem.put("invoices_count", 0);
        runHealthItem.put("failure_count", 0);
        runHealthItem.put("total_amount", 0);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("summary", Map.of(
                "total_runs", 0,
                "invoice_count", 0,
                "failure_rate_pct", 0.0
        ));
        payload.put("run_health", Map.of(
                "last_due_preview_run", runHealthItem,
                "last_invoice_gen_run", runHealthItem,
                "last_mock_charge_run", runHealthItem,
                "last_actual_charge_run", runHealthItem
        ));
        payload.put("forecast", Map.of(
                "due_7_days", Map.of("count", 0, "amount", 0),
                "due_30_days", Map.of("count", 0, "amount", 0),
                "due_90_days", Map.of("count", 0, "amount", 0)
        ));
        payload.put("trends", Map.of(
                "billed_collected", List.of(),
                "run_starts_7d", List.of(),
                "realization_7d", List.of(),
                "stage_distribution", List.of()
        ));
        payload.put("locations", Map.of("top_revenue_locations", List.of()));
        payload.put("contracts", Map.of("frequency_mix", List.of()));
        payload.put("charts", Map.of(
                "payment_method_split", Map.of("segments", List.of()),
                "collection_by_gateway", Map.of("segments", List.of()),
                "ar_aging", Map.of("segments", List.of()),
                "invoice_status", Map.of("segments", List.of()),
                "funnel", Map.of("stages", List.of()),
                "failed_payments", List.of(),
                "alerts", List.of()
        ));
        payload.put("recent_runs", Map.of(
                "rows", List.of(),
                "total", 0,
                "limit", 10,
                "offset", 0
        ));

        when(dashboardService.getOverview(
                any(), any(), any(), any(), any(), anyBoolean(), any(), any(),
                anyInt(), anyInt(), anyInt(), anyInt()))
                .thenReturn(payload);

        mockMvc.perform(get("/api/v1/billing/dashboard/overview")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.summary").exists())
                .andExpect(jsonPath("$.run_health").exists())
                .andExpect(jsonPath("$.forecast").exists())
                .andExpect(jsonPath("$.trends").exists())
                .andExpect(jsonPath("$.locations").exists())
                .andExpect(jsonPath("$.contracts").exists())
                .andExpect(jsonPath("$.charts").exists())
                .andExpect(jsonPath("$.recent_runs").exists())
                .andExpect(jsonPath("$.run_health.last_due_preview_run.invoices_count").value(0))
                .andExpect(jsonPath("$.run_health.last_invoice_gen_run.failure_count").value(0))
                .andExpect(jsonPath("$.run_health.last_mock_charge_run.total_amount").value(0))
                .andExpect(jsonPath("$.run_health.last_actual_charge_run.status").isEmpty());
    }
}

