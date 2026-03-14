package io.clubone.billing.api.v2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.clubone.billing.api.baseobject.Docs;
import io.clubone.billing.api.baseobject.Version;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@Tag(name = "Health Check", description = "View, configured health checkings data")
public class HealthCheckController {

	@Autowired(required = false)
	private BuildProperties buildProperties;

	@GetMapping({"/crm/health"})
	public Version getVersion(HttpServletRequest httpServletRequest) {
		log.debug("inside getVersion() method start");
		Docs docs = new Docs();
		String buildTime = (buildProperties != null && buildProperties.get("time") != null)
				? String.valueOf(buildProperties.get("time"))
				: "N/A";
		docs.setStatus("Live - " + buildTime);
		docs.setUrl(httpServletRequest.getRequestURL().toString().replace("version", "swagger-ui.html"));
		Version version = new Version();
		version.setVersion(getClass().getPackage().getImplementationVersion());
		version.setDocs(docs);
		log.debug("inside getVersion() method end");
		return version;
	}
}
