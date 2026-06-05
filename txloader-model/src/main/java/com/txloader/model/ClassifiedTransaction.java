package com.txloader.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClassifiedTransaction {

    @JsonProperty("date")
    private String date;

    @JsonProperty("merchant")
    private String merchant;

    @JsonProperty("amount")
    private String amount;

    @JsonProperty("category")
    private String category;

    @JsonProperty("account_id")
    private int accountId;

    @JsonProperty("raw_desc")
    private String rawDesc;
}
