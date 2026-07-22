package com.txloader.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Calls an external Transaction Classifier API's POST /predict endpoint to
 * classify a transaction, following the OpenAPI contract:
 * request {description, amount, date} -> response {category, subcategory,
 * confidence, category_confidence, subcategory_confidence}.
 *
 * Any failure (bad input, connection error, timeout, non-200, malformed
 * response) is logged and degraded to CategoryResult.empty() so a single
 * classifier outage can never crash the streaming job.
 */
public class HttpMerchantCategorizer implements MerchantCategorizer {

    private static final Logger LOG = LoggerFactory.getLogger(HttpMerchantCategorizer.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

    private final String classifierUrl;

    private transient HttpClient httpClient;
    private transient ObjectMapper objectMapper;

    public HttpMerchantCategorizer(String classifierUrl) {
        if (classifierUrl == null || classifierUrl.isBlank()) {
            throw new IllegalArgumentException("--classifier-url is required when --classifier http is selected");
        }
        this.classifierUrl = classifierUrl.endsWith("/")
                ? classifierUrl.substring(0, classifierUrl.length() - 1)
                : classifierUrl;
    }

    @Override
    public void open() {
        httpClient = HttpClient.newBuilder()
                // Force HTTP/1.1: the classifier's dev server does not handle
                // the JDK client's default HTTP/2 cleartext upgrade attempt
                // correctly, which silently drops the POST body (422 "missing").
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
        objectMapper = new ObjectMapper();
        LOG.info("HttpMerchantCategorizer opened: classifierUrl={}", classifierUrl);
    }

    @Override
    public CategoryResult categorize(String isoDate, String merchant, String amount, String rawDesc) {
        double amountValue;
        try {
            amountValue = Double.parseDouble(amount);
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse amount '{}' for merchant '{}', skipping classifier call", amount, merchant);
            return CategoryResult.empty();
        }

        try {
            PredictRequest request = new PredictRequest(rawDesc, amountValue, isoDate);
            String requestBody = objectMapper.writeValueAsString(request);

            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(classifierUrl + "/predict"))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (httpResponse.statusCode() != 200) {
                LOG.warn("Classifier API returned status {} for merchant '{}': {}",
                        httpResponse.statusCode(), merchant, httpResponse.body());
                return CategoryResult.empty();
            }

            PredictResponse response = objectMapper.readValue(httpResponse.body(), PredictResponse.class);
            return CategoryResult.of(response.getCategory(), response.getSubcategory(),
                    response.getConfidence(), response.getCategoryConfidence(), response.getSubcategoryConfidence());
        } catch (Exception e) {
            LOG.warn("Classifier API call failed for merchant '{}': {}", merchant, e.toString());
            return CategoryResult.empty();
        }
    }
}
