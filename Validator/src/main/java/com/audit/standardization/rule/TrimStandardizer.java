package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import org.apache.commons.lang.StringUtils;

public class TrimStandardizer implements StandardRule{

    private AlterBean alterBean = new AlterBean();

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        if (val != null) {
            String trim = StringUtils.trim(val.toString());
            alterBean.setValue(trim);
        }

        return alterBean;
    }
}
