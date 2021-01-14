package com.audit.utils;

public interface PolicyPropertyTypes {


    String CURRENT_FEED_VALUE = "#currentFeed";

    enum PROPERTY_TYPE {
        number, string, select, regex, date, chips, feedChips, currentFeed, currentFeedCronSchedule, feedSelect, email, emails, cron, velocityTemplate
    }
}