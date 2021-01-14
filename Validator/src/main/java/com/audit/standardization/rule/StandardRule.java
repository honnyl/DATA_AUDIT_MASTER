package com.audit.standardization.rule;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;

public interface StandardRule {
    AlterBean validateRowValue(Object val, String fieldName, ValidateCondition validateCondition);
}
