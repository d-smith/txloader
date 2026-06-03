package com.txloader.processor;

import com.txloader.model.RawTransaction;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

/**
 * Converts the raw date string to ISO format (YYYY-MM-DD) and extracts
 * a cleaned merchant name from the raw description.
 *
 * Output is a String[] of [isoDate, merchant, amount, rawDesc] passed
 * to TransactionClassifier as an intermediate carrier.
 */
public class TransactionNormalizer implements MapFunction<RawTransaction, String[]> {

    // Handles M/d/yyyy and MM/dd/yyyy formats from the source CSV
    private static final DateTimeFormatter INPUT_DATE_FORMAT = new DateTimeFormatterBuilder()
            .appendOptional(DateTimeFormatter.ofPattern("M/d/yyyy"))
            .appendOptional(DateTimeFormatter.ofPattern("MM/dd/yyyy"))
            .toFormatter(Locale.US);

    private static final DateTimeFormatter OUTPUT_DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    @Override
    public String[] map(RawTransaction raw) {
        String isoDate = normalizeDate(raw.getDate());
        String merchant = extractMerchant(raw.getDescription());
        return new String[]{isoDate, merchant, raw.getAmount(), raw.getDescription(), raw.getAccount()};
    }

    private String normalizeDate(String rawDate) {
        try {
            LocalDate date = LocalDate.parse(rawDate.trim(), INPUT_DATE_FORMAT);
            return date.format(OUTPUT_DATE_FORMAT);
        } catch (Exception e) {
            // Return as-is if already normalized or unparseable
            return rawDate.trim();
        }
    }

    /**
     * Extracts the merchant name as the first whitespace-delimited token of the
     * description, stripping phone numbers and location suffixes that follow.
     */
    private String extractMerchant(String description) {
        if (description == null || description.isBlank()) return "UNKNOWN";
        // Take everything up to the first sequence of multiple spaces or a digit run
        String[] parts = description.trim().split("\\s{2,}|\\s+\\d{3}-\\d{3}");
        return parts[0].trim().toUpperCase(Locale.US);
    }
}
