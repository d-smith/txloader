package com.txloader.processor;

import java.util.Optional;

public record CategoryResult(Optional<String> category, Optional<String> subcategory) {

    public static CategoryResult of(String category) {
        return new CategoryResult(Optional.of(category), Optional.empty());
    }

    public static CategoryResult of(String category, String subcategory) {
        return new CategoryResult(Optional.of(category), Optional.of(subcategory));
    }

    public static CategoryResult empty() {
        return new CategoryResult(Optional.empty(), Optional.empty());
    }
}
