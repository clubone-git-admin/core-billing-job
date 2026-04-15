package io.clubone.billing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.security.oauth2.resource.servlet.OAuth2ResourceServerAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
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
@EnableAsync
@EnableScheduling
public class BillingBatchApplication {
	public static void main(String[] args) {
		SpringApplication.run(BillingBatchApplication.class, args);
	}

	@Bean
	public WebMvcConfigurer corsConfigurer() {
		return new WebMvcConfigurer() {
			@Override
			public void addCorsMappings(CorsRegistry registry) {
				registry.addMapping("/**")
						.allowedOriginPatterns("*")
						.allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "HEAD")
						.allowedHeaders("*")
						.exposedHeaders("Content-Type", "Content-Length", "Location", "X-Request-Id", "X-Application-Id", "X-Location-Id", "X-Actor-Id")
						.allowCredentials(false)
						.maxAge(3600);
			}
		};
	}
}
