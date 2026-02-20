package io.clubone.billing.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Service for S3 operations. Uses credentials and settings from application properties.
 */
@Service
public class S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final String bucketName;
    private final S3Client s3Client;

    public S3Service(
            @Value("${aws.s3.bucket-name}") String bucketName,
            @Value("${aws.s3.region}") String region,
            @Value("${aws.s3.access-key-id}") String accessKeyId,
            @Value("${aws.s3.secret-access-key}") String secretAccessKey) {
        this.bucketName = bucketName;
        this.s3Client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                .build();
    }

    /**
     * Upload content to S3 and return the S3 path.
     *
     * @param content     The content to upload
     * @param fileName    The file name (without path)
     * @param contentType The content type (e.g., "text/csv", "application/json")
     * @return The S3 path (s3://bucket-name/...)
     */
    public String uploadToS3(String content, String fileName, String contentType) {
        try {
            LocalDateTime now = LocalDateTime.now();
            String datePath = now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            String timestamp = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            String s3Key = String.format("billing-preview/%s/%s-%s", datePath, timestamp, fileName);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType(contentType)
                    .build();

            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(
                    new ByteArrayInputStream(contentBytes), contentBytes.length));

            String s3Path = String.format("s3://%s/%s", bucketName, s3Key);
            log.info("Successfully uploaded file to S3: {}", s3Path);
            return s3Path;

        } catch (S3Exception e) {
            log.error("Failed to upload file to S3: fileName={}, error={}", fileName, e.getMessage(), e);
            throw new RuntimeException("Failed to upload file to S3: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error uploading file to S3: fileName={}", fileName, e);
            throw new RuntimeException("Unexpected error uploading file to S3", e);
        }
    }

    /**
     * Upload byte array content to S3.
     *
     * @param content     The content bytes
     * @param fileName    The file name
     * @param contentType The content type
     * @return The S3 path (s3://...)
     */
    public String uploadToS3(byte[] content, String fileName, String contentType) {
        try {
            LocalDateTime now = LocalDateTime.now();
            String datePath = now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            String timestamp = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            String s3Key = String.format("billing-preview/%s/%s-%s", datePath, timestamp, fileName);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType(contentType)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(
                    new ByteArrayInputStream(content), content.length));

            String s3Path = String.format("s3://%s/%s", bucketName, s3Key);
            log.info("Successfully uploaded file to S3: {}", s3Path);
            return s3Path;

        } catch (S3Exception e) {
            log.error("Failed to upload file to S3: fileName={}, error={}", fileName, e.getMessage(), e);
            throw new RuntimeException("Failed to upload file to S3: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error uploading file to S3: fileName={}", fileName, e);
            throw new RuntimeException("Unexpected error uploading file to S3", e);
        }
    }

    /**
     * Download file content from S3 by path.
     *
     * @param s3Path Full S3 path (e.g. s3://bucket-name/key/path/file.csv)
     * @return File content as string (UTF-8)
     */
    public String downloadFromS3(String s3Path) {
        if (s3Path == null || !s3Path.startsWith("s3://")) {
            throw new IllegalArgumentException("Invalid S3 path: " + s3Path);
        }
        String withoutScheme = s3Path.substring(5); // after "s3://"
        int firstSlash = withoutScheme.indexOf('/');
        if (firstSlash <= 0) {
            throw new IllegalArgumentException("Invalid S3 path (missing key): " + s3Path);
        }
        String bucket = withoutScheme.substring(0, firstSlash);
        String key = withoutScheme.substring(firstSlash + 1);

        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            ResponseInputStream<GetObjectResponse> response = s3Client.getObject(getRequest);
            byte[] bytes = response.readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (S3Exception e) {
            log.error("Failed to download from S3: path={}, error={}", s3Path, e.getMessage(), e);
            throw new RuntimeException("Failed to download from S3: " + e.getMessage(), e);
        } catch (IOException e) {
            log.error("Failed to read S3 response: path={}", s3Path, e);
            throw new RuntimeException("Failed to read from S3", e);
        }
    }
}
