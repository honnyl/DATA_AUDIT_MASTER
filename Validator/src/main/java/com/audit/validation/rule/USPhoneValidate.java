package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

public class USPhoneValidate extends RegexValidate implements ValidateRule{

    private AlterBean alterBean = new AlterBean();

    public USPhoneValidate() {
        super("^\\s*(?:\\+?(\\d{1,3}))?[-. (]*(\\d{3})[-. )]*(\\d{3})[-. ]*(\\d{4})(?: *x(\\d+))?\\s*$");
    }

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        String result = "";
        alterBean.setInvalidateError(result);
        if (val != null) {

            USPhoneValidate usPhoneValidate = new USPhoneValidate();

            boolean flag = usPhoneValidate.validate(val.toString());

            if (!flag) {
                alterBean.setInvalidateError(fieldName + "字段手机号格式不正确；");
            }
        }

        return alterBean;
    }
}
