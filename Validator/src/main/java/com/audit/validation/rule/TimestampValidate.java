package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampValidate implements ValidateRule{

    private static final Logger log = LoggerFactory.getLogger(TimestampValidate.class);

    private static final DateTimeFormatter DATETIME_NANOS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter DATETIME_MILLIS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter DATETIME_NOMILLIS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATETIME_NDAY = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_ISO8601 = ISODateTimeFormat.dateTimeParser();
    private static final int MIN_LENGTH = 19;
    private static final int MAX_LENGTH = 29;
    private AlterBean alterBean = new AlterBean();
    private String fieldName;

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        this.fieldName = fieldName;


        String result = "";
        alterBean.setInvalidateError(result);

        if (val != null) {

            validate(val.toString());
        }

        return alterBean;
    }


    public boolean validate(String value) {
        if (!StringUtils.isEmpty(value)) {
            try {
                parseTimestamp(value);
                return true;

            } catch (IllegalArgumentException e) {
                alterBean.setInvalidateError(fieldName + "时间戳取值错误；");
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Parses the string date and returns the
     * Sqoop treats null values as \N.
     */
    public DateTime parseTimestamp(String value) {
        // Check if the value is consider a null
        if ((true) && (value.toUpperCase().equals("NULL"))) {
            return new DateTime();
        }

        int cnt = value.length();
        if (cnt < MIN_LENGTH || cnt > MAX_LENGTH) {
            alterBean.setInvalidateError(fieldName + "字段时间戳长度错误；");
            return new DateTime();
        } else {
            if (value.charAt(10) == 'T') {
                return DATETIME_ISO8601.parseDateTime(value);
            } else if (cnt == MIN_LENGTH) {
                return DATETIME_NOMILLIS.parseDateTime(value);
            } else if (cnt == MAX_LENGTH) {
                return DATETIME_NANOS.parseDateTime(value);
            } else {
                return DATETIME_MILLIS.parseDateTime(value);
            }
        }
    }
}
