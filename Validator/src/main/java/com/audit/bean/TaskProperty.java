package com.audit.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.List;


@Data
public class TaskProperty implements Serializable {

    private String fieldName;

    private String fieldType;

    private List<ValidateCondition> standardList; //标准规则集合

    private List<ValidateCondition> validateList;

}
