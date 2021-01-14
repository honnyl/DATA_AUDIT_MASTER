package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

public class EmailValidate extends RegexValidate implements ValidateRule{

    private AlterBean alterBean = new AlterBean();

    public EmailValidate() {
        super("^([\\w\\d\\-\\.]+)@{1}(([\\w\\d\\-]{1,67})|([\\w\\d\\-]+\\.[\\w\\d\\-]{1,67}))\\.(([a-zA-Z\\d]{2,4})(\\.[a-zA-Z\\d]{2})?)$");
    }

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        String result = "";
        alterBean.setInvalidateError(result);

        if (val != null) {

            EmailValidate emailValidate = new EmailValidate();

            boolean flag = emailValidate.validate(val.toString());

            if (!flag) {
                alterBean.setInvalidateError(fieldName + "字段邮箱格式不正确；");
            }
        }

        return alterBean;
    }
}
