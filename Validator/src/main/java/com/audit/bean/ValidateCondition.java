package com.audit.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @Description: 稽核及加工的实体类
 * @Author: weisc
 * @CreateDate: 2020/6/1 10:34
 * @Version: 1.0
 */

@Data
public class ValidateCondition implements Serializable {


    //规则名称
    private String name;

    private String displayName;

    private String description;

    private String shortDescription;

    private List<ValidateProperty> properties;

    private String objectClassType;

    private String objectShortClassType;

    private String propertyValuesDisplayString;

    private String sequence;


}
