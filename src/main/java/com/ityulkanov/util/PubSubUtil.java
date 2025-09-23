package com.ityulkanov.util;

public class PubSubUtil {
    private PubSubUtil() {
    }

    public static String getFullSubscriptionName(String project, String subscriptionName) {
        String subscription = "projects/" + project + "/subscriptions/";
        return subscription + subscriptionName;
    }

    public static String getFullTopicName(String project, String topicName) {
        String topic = "projects/" + project + "/topics/";
        return topic + topicName;
    }
}
