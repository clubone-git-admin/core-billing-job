package io.clubone.billing.batch.listener;

import io.clubone.billing.batch.model.DueInvoiceRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.stereotype.Component;

/**
 * Item read listener to log when items are read from the database.
 * Helps debug issues with reader returning no records.
 */
@Component
public class BillingItemReadListener implements ItemReadListener<DueInvoiceRow> {

    private static final Logger log = LoggerFactory.getLogger(BillingItemReadListener.class);
    private int readCount = 0;

    @Override
    public void beforeRead() {
        // Called before each read attempt
    }

    @Override
    public void afterRead(DueInvoiceRow item) {
        readCount++;
        if (readCount <= 10) { // Log first 10 items
            log.info("Read invoice: invoiceId={} subscriptionInstanceId={} paymentDueDate={} totalAmount={}", 
                item.getInvoiceId(), item.getSubscriptionInstanceId(), item.getPaymentDueDate(), item.getTotalAmount());
        } else if (readCount == 11) {
            log.info("... (suppressing further read logs, total read so far: {})", readCount);
        }
    }

    @Override
    public void onReadError(Exception ex) {
        log.error("Error reading item", ex);
    }
}
