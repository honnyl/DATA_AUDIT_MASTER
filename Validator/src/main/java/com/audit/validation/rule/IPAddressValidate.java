package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

public class IPAddressValidate extends RegexValidate implements ValidateRule{

    private AlterBean alterBean = new AlterBean();

    public IPAddressValidate() {
        super("^(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])$");
    }

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        String result = "";
        alterBean.setInvalidateError(result);

        if (val != null) {

            if (val.toString() != "") {

                IPAddressValidate ipAddressValidate = new IPAddressValidate();

                boolean flag = ipAddressValidate.validate(val.toString());

                if (!flag) {
                    alterBean.setInvalidateError("ip格式不正确");
                }
            }
        }

        return alterBean;
    }
}
