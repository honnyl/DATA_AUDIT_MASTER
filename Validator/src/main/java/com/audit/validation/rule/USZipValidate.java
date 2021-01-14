package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

public class USZipValidate extends RegexValidate implements ValidateRule{

    private AlterBean alterBean = new AlterBean();

    public USZipValidate() {
        super("^[0-9]{6}$");
    }
    //"^[0-9]{6}$"
    //"[0-9]{5}([- /]?[0-9]{4})?$"

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        String result = "";
        alterBean.setInvalidateError(result);

        if (val != null) {

            USZipValidate usZipValidate = new USZipValidate();

            boolean flag = usZipValidate.validate(val.toString());

            if (!flag) {
                alterBean.setInvalidateError(fieldName + "字段邮政编码格式不正确；");
            }
        }
        return alterBean;
    }
}
