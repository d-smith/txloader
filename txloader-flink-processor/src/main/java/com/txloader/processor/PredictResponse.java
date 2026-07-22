package com.txloader.processor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
class PredictResponse {

    @JsonProperty("category")
    private String category;

    @JsonProperty("subcategory")
    private String subcategory;

    @JsonProperty("confidence")
    private double confidence;

    @JsonProperty("category_confidence")
    private double categoryConfidence;

    @JsonProperty("subcategory_confidence")
    private double subcategoryConfidence;
}
