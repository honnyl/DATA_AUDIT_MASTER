package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

import java.util.regex.Pattern;

public class StripNonNumeric implements StandardRule{

    private AlterBean alterBean = new AlterBean();

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        Pattern pattern = Pattern.compile("[^-+\\d.]");

        if (val != null) {
            String str = pattern.matcher(val.toString()).replaceAll("");

            alterBean.setValue(str);
        }

        return alterBean;
    }

    public String test(Object val) {


        Pattern pattern = Pattern.compile("[^-+\\d.]");

        String str = "";
        if (val != null) {
            str = pattern.matcher(val.toString()).replaceAll("");
        }

        return str;
    }
}
