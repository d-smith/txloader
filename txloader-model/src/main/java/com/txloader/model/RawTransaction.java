package com.txloader.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RawTransaction {

    @JsonProperty("date")
    private String date;

    @JsonProperty("account")
    private String account;

    @JsonProperty("description")
    private String description;

    @JsonProperty("amount")
    private String amount;

    public RawTransaction() {}

    public RawTransaction(String date, String account, String description, String amount) {
        this.date = date;
        this.account = account;
        this.description = description;
        this.amount = amount;
    }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public String getAccount() { return account; }
    public void setAccount(String account) { this.account = account; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getAmount() { return amount; }
    public void setAmount(String amount) { this.amount = amount; }

    @Override
    public String toString() {
        return "RawTransaction{date='" + date + "', account='" + account +
               "', description='" + description + "', amount='" + amount + "'}";
    }
}
