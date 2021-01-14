package com.audit.bean;


import lombok.Data;

@Data
public class AlterBean {

    private String invalidateError = "";

    private Object value;
}
