package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.bean.ValidateProperty;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class LengthValidate implements ValidateRule{

    private static final Logger log = LoggerFactory.getLogger(LengthValidate.class);

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        String result = "";
        AlterBean alterBean = new AlterBean();
        alterBean.setInvalidateError(result);

        if (val != null) {

            List<ValidateProperty> properties = validateCondition.getProperties();

            int size = properties.size();

            boolean max_required = false;
            boolean min_required = false;

            if (size > 0) {

                if (size == 1) {

                    max_required = properties.get(0).getName().equals("最大长度");
                    min_required = properties.get(0).getName().equals("最小长度");
                } else if (size == 2) {
                    max_required = true;
                    min_required = true;
                }


                if (max_required) {
                    int max = Integer.parseInt(properties.get(0).getValue());
                    if (min_required) {
                        int min = Integer.parseInt(properties.get(1).getValue());
                        if (min < 0 || min > max) {
                            log.error("min > max，参数有误！");
                        } else {
                            if (val.toString().length() > max || val.toString().length() < min) {
                                alterBean.setInvalidateError(fieldName + "字段不符合规定长度范围；");
                            }
                        }
                    } else {
                        alterBean.setInvalidateError(fieldName + "字段长度大于最大长度；");
                    }
                } else {
                    int min = Integer.parseInt(properties.get(0).getValue());
                    if (min < 0) {
                        log.error("min < 0，参数有误！");
                    } else {
                        if (val.toString().length() < min) {
                            alterBean.setInvalidateError(fieldName + "字段长度小于最小长度；");
                        }
                    }
                }
            }
        }

        return alterBean;
    }
}
