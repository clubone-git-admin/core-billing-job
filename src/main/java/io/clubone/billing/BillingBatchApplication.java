package io.clubone.billing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.security.oauth2.resource.servlet.OAuth2ResourceServerAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication(exclude = { BatchAutoConfiguration.class, OAuth2ResourceServerAutoConfiguration.class // Exclude
																												// by
																												// default,
																												// enable
																												// via
																												// SecurityConfig
																												// when
																												// needed
})
@EnableRetry
public class BillingBatchApplication {
	public static void main(String[] args) {
		SpringApplication.run(BillingBatchApplication.class, args);
	}

	@Bean
	public WebMvcConfigurer corsConfigurer() {
		return new WebMvcConfigurer() {
			@Override
			public void addCorsMappings(CorsRegistry registry) {
				registry.addMapping("/**") // Adjust the mapping pattern as per your requirements
						.allowedOriginPatterns("*") // Set the allowed origins or "*" for all origins
						.allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH") // Set the allowed HTTP methods
						.allowedHeaders("*")// Set the allowed headers
						.allowCredentials(true); // Allow credentials, if needed
			}
		};
	}
}
