# Use Amazon Corretto 21 (virtual threads + modern GC)
FROM amazoncorretto:21

WORKDIR /app

COPY target/core-billing-job-0.1.0-SNAPSHOT.jar /app/core-billing-job.jar

EXPOSE 8000

ENV SERVER_PORT=8000

# Cap heap to container RAM; exit on OOM so ECS/K8s restarts cleanly instead of thrashing.
# MaxRAMPercentage leaves headroom for metaspace / native / direct buffers.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70.0 -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -XX:+UseG1GC"

CMD ["java", "-jar", "/app/core-billing-job.jar", "--server.port=8000"]
