package io.clubone.billing.config;

import io.clubone.billing.api.context.CrmContextInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers CRM context interceptor for /api/crm/** so orgClientId and actorId are set from headers.
 */
@Configuration
public class CrmWebMvcConfig implements WebMvcConfigurer {

    private final CrmContextInterceptor crmContextInterceptor;

    public CrmWebMvcConfig(CrmContextInterceptor crmContextInterceptor) {
        this.crmContextInterceptor = crmContextInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(crmContextInterceptor)
                .addPathPatterns("/api/crm/**");
    }
}
