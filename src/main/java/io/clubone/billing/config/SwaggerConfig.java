package io.clubone.billing.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;

@Configuration
public class SwaggerConfig {

  @Value("${swagger.title:Billing Job APIs}")
  private String title;

  @Value("${swagger.description:Billing runs, approvals, DLQ, reconciliation, and dashboards}")
  private String description;

  @Value("${swagger.version:V1}")
  private String version;

  @Value("${swagger.contact.name:ClubOne Team}")
  private String contactName;

  @Value("${swagger.contact.url:https://www.clubone.com}")
  private String contactURL;

  @Value("${swagger.contact.email:ask@clubone.com}")
  private String contactEmail;

  @Value("${swagger.license:Apache License 2.0}")
  private String license;

  @Value("${swagger.licenseUrl:https://www.apache.org/licenses/LICENSE-2.0}")
  private String licenseURL;

  @Bean
  public OpenAPI api() {
    final String actorScheme = "X-Actor-Id";
    final String locationScheme = "X-Location-Id";
    final String appScheme = "application-id";

    return new OpenAPI()
        .info(new Info()
            .title(title)
            .description(description)
            .version(version)
            .contact(new Contact().name(contactName).url(contactURL).email(contactEmail))
            .license(new License().name(license).url(licenseURL)))
        .components(new Components()
            .addSecuritySchemes(actorScheme, headerScheme(actorScheme, "Actor application_user_id (UUID)"))
            .addSecuritySchemes(locationScheme, headerScheme(locationScheme, "Working location_id (UUID)"))
            .addSecuritySchemes(appScheme, headerScheme(appScheme, "Tenant application_id (UUID)"))
            .addParameters(actorScheme, new Parameter().in("header").name(actorScheme).required(true))
            .addParameters(locationScheme, new Parameter().in("header").name(locationScheme).required(true))
            .addParameters(appScheme, new Parameter().in("header").name(appScheme).required(true)))
        .security(List.of(new SecurityRequirement()
            .addList(actorScheme)
            .addList(locationScheme)
            .addList(appScheme)));
  }

  private static SecurityScheme headerScheme(String name, String description) {
    return new SecurityScheme()
        .type(SecurityScheme.Type.APIKEY)
        .in(SecurityScheme.In.HEADER)
        .name(name)
        .description(description);
  }
}
