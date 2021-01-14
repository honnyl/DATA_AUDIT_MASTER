package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

public class DefaultValueStandardizer implements StandardRule{

    private AlterBean alterBean = new AlterBean();

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        String defaultValue = validateCondition.getProperties().get(0).getValue();

        if (!val.toString().equals("")) {
            return alterBean;
        } else {
            alterBean.setValue(defaultValue);
        }

        return alterBean;
    }
}
