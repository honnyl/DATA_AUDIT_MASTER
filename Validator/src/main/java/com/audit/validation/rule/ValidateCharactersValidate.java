package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.bean.ValidateProperty;
import org.apache.commons.lang3.StringUtils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ValidateCharactersValidate implements ValidateRule{

    private static final Logger log = LoggerFactory.getLogger(TimestampValidate.class);

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        AlterBean alterBean = new AlterBean();
        String result = "";
        alterBean.setInvalidateError(result);

        List<ValidateProperty> properties = validateCondition.getProperties();

        if (properties.size() != 0) {

            String validationType = properties.get(0).getValue();


            boolean is_not_null = properties.get(0).isRequired();
            if (is_not_null) {
                if (val.toString().equals("")) {
                    alterBean.setInvalidateError(fieldName + "字段值不能为空；");
                }
            }

            if (val != null) {

                String trimmedValue = StringUtils.deleteWhitespace(val.toString());
                if ("大写".equalsIgnoreCase(validationType)) {
                    if (!trimmedValue.toUpperCase().equals(trimmedValue)) {
                        alterBean.setInvalidateError(fieldName + "字段值不符合大写规则；");
                    }
                } else if ("小写".equalsIgnoreCase(validationType)) {
                    if (!trimmedValue.toLowerCase().equals(trimmedValue)) {
                        alterBean.setInvalidateError(fieldName + "字段值不符合小写规则；");
                    }
                } else if ("字母和数字".equalsIgnoreCase(validationType)) {
                    if (!StringUtils.isAlphanumeric(val.toString())) {
                        alterBean.setInvalidateError(fieldName + "字段值不是字母或数字；");
                    }
                } else if ("字母".equalsIgnoreCase(validationType)) {
                    if (!StringUtils.isAlpha(val.toString())) {
                        alterBean.setInvalidateError(fieldName + "字段值不是字母；");
                    }
                } else if ("数字".equalsIgnoreCase(validationType)) {
                    if (!StringUtils.isNumeric(val.toString())) {
                        alterBean.setInvalidateError(fieldName + "字段值不是数字；");
                    }
                } else {
                    log.error("校验标准不在定义值列表中；");
                }
            }
        }
        return alterBean;
    }
}
