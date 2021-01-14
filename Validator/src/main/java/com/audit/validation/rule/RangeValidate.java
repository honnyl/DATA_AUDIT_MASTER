package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.bean.ValidateProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RangeValidate implements ValidateRule{

    private double max;
    private double min;
    private static final Logger log = LoggerFactory.getLogger(LengthValidate.class);

    @Override
    public AlterBean validateRowValue(Object val, String filedName, ValidateCondition validateCondition) {

        AlterBean alterBean = new AlterBean();
        String result = "";
        alterBean.setInvalidateError(result);

        if (val != null) {

            List<ValidateProperty> properties = validateCondition.getProperties();

            boolean max_required = false;
            boolean min_required = false;
            int size = properties.size();

            if (size > 0) {

                /*if (size == 1) {

                    min_required = properties.get(0).getName().equals("最小值");
                    max_required = properties.get(0).getValue().equals("最大值");
                } else if (size == 2) {
                    max_required = true;
                    min_required = true;
                }*/

                min_required = (properties.get(0).getValue() != null);
                max_required = (properties.get(1).getValue() != null);


                if (min_required) {
                    min = Double.parseDouble(properties.get(0).getValue());
                    if (max_required) {
                        max = Double.parseDouble(properties.get(1).getValue());
                        if (min < 0 || min > max) {
                            log.error("min > max，参数有误！");
                        } else {
                            if (Double.valueOf(val.toString()) > max || Double.valueOf(val.toString()) < min) {
                                alterBean.setInvalidateError(filedName + "字段不符合规定取值范围；");
                            }
                        }
                    } else {
                        if (min < 0) {
                            log.error("min < 0，参数有误！");
                        } else {
                            if (Double.valueOf(val.toString()) < min) {
                                alterBean.setInvalidateError(filedName + "字段数值小于最小值；");
                            }
                        }
                    }
                } else {
                    max = Double.parseDouble(properties.get(1).getValue());
                    if (Double.valueOf(val.toString()) > max) {
                        alterBean.setInvalidateError(filedName + "字段数值大于最大值；");
                    }
                }
            }
        }

        return alterBean;
    }
}
