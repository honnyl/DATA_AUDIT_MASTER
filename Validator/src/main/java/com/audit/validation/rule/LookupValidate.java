package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.bean.ValidateProperty;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LookupValidate implements ValidateRule{

    private static final Logger log = LoggerFactory.getLogger(LengthValidate.class);

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        AlterBean alterBean = new AlterBean();
        String result = "";
        alterBean.setInvalidateError(result);

        if (val != null) {

            List<ValidateProperty> properties = validateCondition.getProperties();

            String lookupName = validateCondition.getProperties().get(0).getName();

            String value = validateCondition.getProperties().get(0).getValue();

            switch (lookupName) {
                case "空值检核":
                    if (val == null) {
                        alterBean.setInvalidateError(fieldName + "字段值为空；");
                    }
                    break;
                case "数据格式检核":
                    if (value == null) {
                        log.error("未传入正确的正则表达式！");
                    } else {
                        alterBean = new RegexValidate(value).validateRowValue(val, fieldName, validateCondition);
                    }
                    break;
                case "唯一性检核":
                    SparkSession ssc = SparkSession.builder().getOrCreate();
                    if (val != null) {
                        String sql = "select " + fieldName.toUpperCase() + " from global_temp.OnlyOnceTemp_" + fieldName.toUpperCase() + " where " + fieldName.toUpperCase() + " = " + "'" + val.toString() + "'";
                        Dataset<Row> rowDataset = ssc.sql(sql);
                        long count = rowDataset.count();
                        if (count > 0) {
                            alterBean.setInvalidateError(fieldName + "字段数据重复；");
                        }
                    }
                    break;
                case "准确性检核":
                    String subValue1 = properties.get(0).getSubValue1();
                    String subValue2 = properties.get(0).getSubValue2();
                    String value2 = properties.get(0).getValue();

                    if (val != null) {

                        if (value2.equals("阈值")) {

                            if (Double.parseDouble(val.toString()) < Double.parseDouble(subValue2) && Double.parseDouble(val.toString()) > Double.parseDouble(subValue1)) {

                            } else {
                                alterBean.setInvalidateError(fieldName + "字段不在规定的范围内；");
                            }
                        }

                        if (value2 == "维度") {

                        }
                    }

                    break;
            }
        }

        return alterBean;
    }
}
