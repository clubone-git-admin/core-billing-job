package io.clubone.billing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;

@SpringBootApplication(exclude = {BatchAutoConfiguration.class})
public class BillingBatchApplication {
  public static void main(String[] args) {
    SpringApplication.run(BillingBatchApplication.class, args);
  }
}
