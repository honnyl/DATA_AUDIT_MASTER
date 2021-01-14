package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.validation.rule.LengthValidate;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;


@Data
public class SimpleRegexReplacer implements StandardRule{

    private AlterBean alterBean = new AlterBean();
    private String inputPattern;
    private Pattern pattern;
    private String replacement = "";
    private static final Logger log = LoggerFactory.getLogger(LengthValidate.class);


    public SimpleRegexReplacer() {
    }

    public SimpleRegexReplacer(String inputPattern, String replacement) {
        this.inputPattern = inputPattern;
        this.replacement = replacement;
        pattern = Pattern.compile(inputPattern);
    }

    @Override
    public AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition) {
        String result = "";
        alterBean.setValue(val);
        alterBean.setInvalidateError(result);

        replacement = validateCondition.getProperties().get(1).getValue();
        inputPattern = validateCondition.getProperties().get(0).getValue();

        if (replacement == null) {
            String str = pattern.matcher(val.toString()).replaceAll("");
            alterBean.setValue(str);
        }

        try {
            pattern = Pattern.compile(inputPattern);
        } catch (PatternSyntaxException e) {
            log.error("正则表达式输入有误！");
        }
        String str = pattern.matcher(val.toString()).replaceAll(replacement);
        alterBean.setValue(str);


        return alterBean;
    }
}
