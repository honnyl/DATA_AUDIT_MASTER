package com.audit.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class ValidateProperty implements Serializable {

    private String name;

    private String displayName;

    private String value;

    private String subValue1;

    private String subValue2;

    private String type;

    private String hint;

    private String objectProperty;

    private List<ValidateValues> selectableValues;

    private boolean required;

    private String group;

    private Integer groupOrder;

    private String layout;

    private boolean hidden;

    private String pattern;

    private String patternInvalidMessage;

    private List<AdditionalProperties> additionalProperties;

}
