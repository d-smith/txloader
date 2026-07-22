package com.txloader.processor;

import java.util.Optional;

public record CategoryResult(
        Optional<String> category,
        Optional<String> subcategory,
        Optional<Double> confidence,
        Optional<Double> categoryConfidence,
        Optional<Double> subcategoryConfidence) {

    public static CategoryResult of(String category) {
        return new CategoryResult(Optional.of(category), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static CategoryResult of(String category, String subcategory) {
        return new CategoryResult(Optional.of(category), Optional.of(subcategory),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static CategoryResult of(String category, String subcategory,
            double confidence, double categoryConfidence, double subcategoryConfidence) {
        return new CategoryResult(Optional.of(category), Optional.of(subcategory),
                Optional.of(confidence), Optional.of(categoryConfidence), Optional.of(subcategoryConfidence));
    }

    public static CategoryResult empty() {
        return new CategoryResult(Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }
}
