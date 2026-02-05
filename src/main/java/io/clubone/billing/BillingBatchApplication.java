package io.clubone.billing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.security.oauth2.resource.servlet.OAuth2ResourceServerAutoConfiguration;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication(exclude = {
    BatchAutoConfiguration.class,
    OAuth2ResourceServerAutoConfiguration.class  // Exclude by default, enable via SecurityConfig when needed
})
@EnableRetry
public class BillingBatchApplication {
  public static void main(String[] args) {
    SpringApplication.run(BillingBatchApplication.class, args);
  }
}
