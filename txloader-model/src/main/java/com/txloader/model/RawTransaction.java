package com.txloader.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawTransaction {

    @JsonProperty("date")
    private String date;

    @JsonProperty("account")
    private String account;

    @JsonProperty("description")
    private String description;

    @JsonProperty("amount")
    private String amount;
}
