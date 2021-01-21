package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.bean.ValidateProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NotNullValidate implements ValidateRule{

    private static final Logger log = LoggerFactory.getLogger(LengthValidate.class);

    @Override
    public AlterBean validateRowValue(Object val, String filedName, ValidateCondition validateCondition) {

        AlterBean alterBean = new AlterBean();
        String result = "";
        alterBean.setInvalidateError(result);

        List<ValidateProperty> properties = validateCondition.getProperties();

        //boolean is_not_null = Boolean.parseBoolean(properties.get(0).getValue());

        //if (is_not_null) {
            if (val == null){

                alterBean.setInvalidateError(filedName + "字段值不能为空；");
            } else {
                if (val.toString().length() == 0)
                    alterBean.setInvalidateError(filedName + "字段值不能为空；");
            }
        //}

        /*boolean is_trim = Boolean.parseBoolean(properties.get(1).getValue());
        if (is_trim) {
            if (val.toString().equals("")){

                if (alterBean.getInvalidateError()!= null) {
                    alterBean.setInvalidateError(alterBean.getInvalidateError() + filedName + "字段值为空,不能切分；");
                }
            }
        }*/

        return alterBean;
    }
}
