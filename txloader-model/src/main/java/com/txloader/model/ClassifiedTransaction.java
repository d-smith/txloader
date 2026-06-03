package com.txloader.model;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    public ClassifiedTransaction() {}

    public ClassifiedTransaction(String date, String merchant, String amount,
                                  String category, int accountId, String rawDesc) {
        this.date = date;
        this.merchant = merchant;
        this.amount = amount;
        this.category = category;
        this.accountId = accountId;
        this.rawDesc = rawDesc;
    }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public String getMerchant() { return merchant; }
    public void setMerchant(String merchant) { this.merchant = merchant; }

    public String getAmount() { return amount; }
    public void setAmount(String amount) { this.amount = amount; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public int getAccountId() { return accountId; }
    public void setAccountId(int accountId) { this.accountId = accountId; }

    public String getRawDesc() { return rawDesc; }
    public void setRawDesc(String rawDesc) { this.rawDesc = rawDesc; }

    @Override
    public String toString() {
        return "ClassifiedTransaction{date='" + date + "', merchant='" + merchant +
               "', amount='" + amount + "', category='" + category +
               "', accountId=" + accountId + ", rawDesc='" + rawDesc + "'}";
    }
}
