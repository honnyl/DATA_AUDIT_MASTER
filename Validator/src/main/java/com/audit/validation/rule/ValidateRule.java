package com.audit.validation.rule;

import com.audit.bean.*;


public interface ValidateRule {
    AlterBean validateRowValue(Object val, String filedName, ValidateCondition validateCondition);
}
