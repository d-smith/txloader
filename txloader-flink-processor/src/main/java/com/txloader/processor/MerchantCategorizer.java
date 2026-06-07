package com.txloader.processor;

public interface MerchantCategorizer extends java.io.Serializable {

    default void open() throws Exception {}

    String categorize(String merchant);

    default void close() throws Exception {}
}
