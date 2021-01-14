package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

public class UppercaseStandardizer implements StandardRule{

    private AlterBean alterBean = new AlterBean();

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        if (!val.toString().equals("")) {
            alterBean.setValue(val.toString().toUpperCase());
        } else alterBean.setInvalidateError(fieldName + "为空");

        return alterBean;
    }
}
