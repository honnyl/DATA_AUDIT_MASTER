package com.audit.validation.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.utils.PolicyPropertyRef;
import lombok.Data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@Data
public class RegexValidate implements ValidateRule{

    private String regexExpression;
    private Pattern pattern;
    private boolean valid;
    AlterBean alterBean = new AlterBean();
    private String result = "";

    public RegexValidate() {
    }

    public RegexValidate(@PolicyPropertyRef(name = "Regex expression") String regex) {
        try {
            this.regexExpression = regex;
            this.pattern = Pattern.compile(regex);
            valid = true;
        } catch (PatternSyntaxException e) {
            alterBean.setInvalidateError("表达式错误；");
        }
    }

    @Override
    public AlterBean validateRowValue(Object val, String filedName, ValidateCondition validateCondition) {
        return alterBean;
    }

    public boolean validate ( String str) {
        Matcher matcher = pattern.matcher(str);
        if (matcher.matches()) {
            return true;
        } else {
            return false;
        }
    }

}
