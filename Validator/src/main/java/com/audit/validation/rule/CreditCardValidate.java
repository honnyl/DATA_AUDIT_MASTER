package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.utils.CreditCardAuditUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreditCardValidate extends LengthValidate implements ValidateRule{

    private static final Logger log = LoggerFactory.getLogger(CreditCardValidate.class);

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {

        AlterBean alterBean = new AlterBean();

        String result = "";
        if (val != null) {

            if (val.toString().length() < 15 || val.toString().length() > 19) {
                alterBean.setInvalidateError(fieldName + "字段银行卡错误；");
            } else {

                String nameOfBank = CreditCardAuditUtil.getNameOfBank(val.toString().substring(0, 6));

                if (CreditCardAuditUtil.checkBankCard(val.toString()) &&
                        nameOfBank != null && val.toString().length() >= 15 && val.toString().length() <= 19) {
                    result = "";
                    log.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + nameOfBank + "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
                } else {
                    alterBean.setInvalidateError(fieldName + "字段银行卡错误；");
                }
            }
        }

        return alterBean;
    }
}
