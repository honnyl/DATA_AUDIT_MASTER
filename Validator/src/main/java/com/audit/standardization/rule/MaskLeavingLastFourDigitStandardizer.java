package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

import java.util.regex.Pattern;

public class MaskLeavingLastFourDigitStandardizer implements StandardRule{

    private AlterBean alterBean = new AlterBean();

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        Pattern pattern = Pattern.compile("\\d");
        String str;
        if (val != null) {
            if (val.toString().length() > 4) {
                str = pattern.matcher(val.toString().substring(0, val.toString().length() - 4)).replaceAll("*");
                str = str + val.toString().substring(val.toString().length() - 4);
            } else {
                str = val.toString();
            }

            alterBean.setValue(str);
        }

        return alterBean;
    }
}
