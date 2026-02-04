package io.clubone.billing.batch.reader;

import io.clubone.billing.batch.model.DueInvoiceRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.UUID;

public class DueInvoiceRowMapper implements RowMapper<DueInvoiceRow> {

	private static final Logger log = LoggerFactory.getLogger(DueInvoiceRowMapper.class);

	@Override
	public DueInvoiceRow mapRow(ResultSet rs, int rowNum) throws SQLException {
		try {
			DueInvoiceRow r = new DueInvoiceRow();
			r.setInvoiceId((UUID) rs.getObject("invoice_id"));
			r.setSubscriptionInstanceId((UUID) rs.getObject("subscription_instance_id"));
			r.setCycleNumber((Integer) rs.getObject("cycle_number"));
			r.setPaymentDueDate(rs.getObject("payment_due_date", LocalDate.class));
			r.setClientRoleId((UUID) rs.getObject("client_role_id"));
			r.setSubTotal(rs.getBigDecimal("sub_total"));
			r.setTaxAmount(rs.getBigDecimal("tax_amount"));
			r.setDiscountAmount(rs.getBigDecimal("discount_amount"));
			r.setTotalAmount(rs.getBigDecimal("total_amount"));
			r.setClientPaymentMethodId((UUID) rs.getObject("client_payment_method_id"));
			return r;
		} catch (SQLException e) {
			log.error("Failed to map due invoice row {}: {}", rowNum, e.getMessage(), e);
			throw e;
		}
	}
}
