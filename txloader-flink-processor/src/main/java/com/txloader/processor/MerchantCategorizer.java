package com.txloader.processor;

public interface MerchantCategorizer extends java.io.Serializable {

    default void open() throws Exception {}

    CategoryResult categorize(String isoDate, String merchant, String amount, String rawDesc);

    default void close() throws Exception {}
}
