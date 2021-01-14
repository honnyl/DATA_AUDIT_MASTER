package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import org.apache.commons.codec.binary.Base64;

import java.util.Arrays;

public class Base64Encode implements StandardRule{

    private AlterBean alterBean = new AlterBean();

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        String output = validateCondition.getProperties().get(0).getValue();

        if (output.equals("二进制")) {
            try {
                byte[] decoded = Base64.encodeBase64(val.toString().getBytes("UTF-8"));
                alterBean.setValue(decoded);
            } catch (Exception e) {
                alterBean.setInvalidateError(fieldName + "字段decodeBase64编码错误");
            }
        } else if (output.equals("字符串")){

            try {
                String str = Base64.encodeBase64URLSafeString(val.toString().getBytes("UTF-8"));
                alterBean.setValue(str);
            } catch (Exception e) {
                alterBean.setInvalidateError(fieldName + "字段decodeBase64解码错误");
            }

        }
        return alterBean;
    }
}
