package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.utils.JodaUtils;
import org.apache.commons.lang3.StringUtils;
import org.datanucleus.store.rdbms.identifier.IdentifierFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalInstantException;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TimeZone;


/*根据提供的输入格式将日期时间转换为Hive使用的ISO8601格式。如果输入格式为null，则日期为
        假设为Java epoch时间，否则使用格式化模式转换日期。*/
public class DateTimeStandardizer implements StandardRule{

    private static final Logger log = LoggerFactory.getLogger(DateTimeStandardizer.class);
    private String inputDateFormat;
    private OutputFormats outputFormat;
    private String inputTimezone;
    private String outputTimezone;
    private transient DateTimeFormatter outputFormatter;
    private transient DateTimeFormatter inputFormatter;
    private AlterBean alterBean = new AlterBean();
    private boolean valid;

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        inputDateFormat = validateCondition.getProperties().get(0).getValue();
        String outputFormatValue = validateCondition.getProperties().get(1).getValue();
        inputTimezone = validateCondition.getProperties().get(2).getValue();
        outputTimezone = validateCondition.getProperties().get(3).getValue();

        switch (outputFormatValue) {
            case "":
                outputFormat = null;
                break;
            case "DATE_ONLY":
                outputFormat = OutputFormats.DATE_ONLY;
                break;
            case "DATETIME":
                outputFormat = OutputFormats.DATETIME;
                break;
            case "DATETIME_NOMILLIS":
                outputFormat = OutputFormats.DATETIME_NOMILLIS;
                break;
            default:
                log.error("outputFormat不在定义值列表中！");
        }

        initializeFormatters();

        String str = convertValue(val.toString());
        alterBean.setValue(str);

        return alterBean;
    }


    public String test(Object val) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        inputDateFormat = "";
        String outputFormatValue ="DATETIME";
        inputTimezone = "AET";
        outputTimezone = "CST";

        switch (outputFormatValue) {
            case "":
                outputFormat = null;
                break;
            case "DATE_ONLY":
                outputFormat = OutputFormats.DATE_ONLY;
                break;
            case "DATETIME":
                outputFormat = OutputFormats.DATETIME;
                break;
            case "DATETIME_NOMILLIS":
                outputFormat = OutputFormats.DATETIME_NOMILLIS;
                break;
            default:
                log.error("outputFormat不在定义值列表中！");
        }

        initializeFormatters();

        String str = convertValue(val.toString());
        alterBean.setValue(str);

        return str;
    }


    //Unix时间戳的单位是秒..检测字符串是否以秒为单位只有10个字符，而不是毫秒
    private boolean isInputUnixTimestamp(String value) {
        return StringUtils.isNotBlank(value) && StringUtils.isNumeric(value) && value.length() == 10;
    }

    public String convertValue(String value) {
        if (valid) {
            try {
                if (inputFormatter == null) {
                    if (isInputUnixTimestamp(value)) {
                        //unix timestamp are in seconds
                        long lValue = Long.parseLong(value);
                        lValue *= 1000;
                        return outputFormatter.print(lValue);
                    } else {
                        long lValue = Long.parseLong(value);
                        return outputFormatter.print(lValue);
                    }
                }

                DateTime dt = inputFormatter.parseDateTime(value);

                return outputFormatter.print(dt);
            } catch (IllegalInstantException e) {
                if ((inputTimezone == null && !JodaUtils.formatContainsTime(this.inputDateFormat)) &&
                        outputFormat == OutputFormats.DATE_ONLY) {
                    // in the case where user is not matching time in the formatString, has not specified the
                    //    input time zone and the output format is DATE_ONLY, it is safe to retry with
                    //    a "timezone-less" date parsing to avoid any time zone offset transition problems
                    //    on the input date.  output date
                    DateTime dt = LocalDate.parse(value, inputFormatter).toDateTimeAtStartOfDay();
                    return outputFormatter.print(dt);
                } else {
                    // otherwise...  we can't be sure how an incoming date in time
                    //    zone offset transition should be handled.  It is best to skip the conversion
                    //    and hope the feed has a validator that would catch the row as invalid, so
                    //    the problem can be addressed at the source.
                    throw e;
                }
            } catch (IllegalArgumentException e) {
                log.debug("Failed to convert string [{}] to date pattern [{}], value, inputDateFormat");
            }
        }
        return value;
    }

    private final void initializeFormatters() {
        try {
            valid = false;
            if (outputFormat == null) {
                outputFormat = OutputFormats.DATE_ONLY;
            }
            switch (outputFormat) {
                case DATE_ONLY:
                    this.outputFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
                    break;
                case DATETIME:
                    this.outputFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
                    break;

                case DATETIME_NOMILLIS:
                    this.outputFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                    break;
            }
            this.outputFormatter = formatterForTimezone(this.outputFormatter, outputTimezone);
            if (StringUtils.isNotBlank(inputDateFormat)) {
                this.inputFormatter = DateTimeFormat.forPattern(this.inputDateFormat);
                this.inputFormatter = formatterForTimezone(this.inputFormatter, inputTimezone);
            }
            valid = true;
        } catch (IllegalArgumentException e) {
            log.warn("Illegal configuration input format [{}], tz [{}] Output format  [{}], tz [{}]"
                    + "]. Standardizer will be skipped.", inputDateFormat, inputTimezone, outputFormat, outputTimezone);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initializeFormatters();
    }


    protected DateTimeFormatter formatterForTimezone(DateTimeFormatter format, String timezone) {

        if (StringUtils.isEmpty(timezone)) {
            return format;
        }
        if ("UTC".equals(timezone)) {
            return format.withZoneUTC();
        }
        return format.withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(timezone)));

    }


    public enum OutputFormats {DATE_ONLY, DATETIME, DATETIME_NOMILLIS}
}
