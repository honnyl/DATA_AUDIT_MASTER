package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateValidate implements ValidateRule{

    private static final Logger log = LoggerFactory.getLogger(DateValidate.class);
    private static final DateTimeFormatter DATE = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final int LENGTH = 19;
    private String result = "";
    private AlterBean alterBean = new AlterBean();

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        if (val != null) {

            if (!StringUtils.isEmpty(val.toString())) {
                try {
                    alterBean.setValue(parseDate(val.toString()));

                } catch (IllegalArgumentException e) {
                    alterBean.setInvalidateError(fieldName + "字段日期格式不正确；");
                }
            }
        }
        return alterBean;
    }


    public DateTime parseDate(String value) {
        int cnt = value.length();
        if (cnt <= LENGTH) {
            return LocalDate.parse(value, DATE).toDateTimeAtStartOfDay();
        } else {
            throw new IllegalArgumentException("Expecting yyyy-MM-dd");
        }
    }
}
