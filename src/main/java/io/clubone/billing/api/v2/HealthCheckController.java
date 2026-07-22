package io.clubone.billing.api.v2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.clubone.billing.api.baseobject.Docs;
import io.clubone.billing.api.baseobject.Version;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Liveness endpoint for ECS/ALB ({@code /crm/health}). Kept allocation-light so it
 * still returns under load when Tomcat threads are constrained.
 */
@RestController
@Tag(name = "Health Check", description = "View, configured health checkings data")
public class HealthCheckController {

	@Autowired(required = false)
	private BuildProperties buildProperties;

	@GetMapping({"/crm/health", "/health"})
	public Version getVersion(HttpServletRequest httpServletRequest) {
		Docs docs = new Docs();
		String buildTime = (buildProperties != null && buildProperties.get("time") != null)
				? String.valueOf(buildProperties.get("time"))
				: "N/A";
		docs.setStatus("Live - " + buildTime);
		docs.setUrl(httpServletRequest.getRequestURL().toString().replace("version", "swagger-ui.html"));
		Version version = new Version();
		version.setVersion(getClass().getPackage().getImplementationVersion());
		version.setDocs(docs);
		return version;
	}
}
