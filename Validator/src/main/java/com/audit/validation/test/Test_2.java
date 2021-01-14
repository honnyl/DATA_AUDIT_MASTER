package com.audit.validation.test;

import lombok.Data;

import java.util.HashMap;

@Data
public class Test_2 {

 /*   String CREDITCARD_PASS;
    String CREDITCARD_NOPASS;
    String DATE_PASS;
    String DATE_NOPASS;
    String EMAIL_PASS;
    String EMAIL_NOPASS;
    String IP_PASS;
    String IP_NOPASS;
    String LENGTH_PASS;
    String LENGTH_NOPASS;
    String LOOKUP_PASS;
    String LOOKUP_NOPASS;
    String NOTNULL_PASS;
    String NOTNULL_NOPASS;
    String TIMESTAMP_PASS;
    String TIMESTAMP_NOPASS;
    String USPHONE_PASS;
    String USPHONE_NOPASS;
    String USZIP_PASS;
    String USZIP_NOPASS;
    String CHARACTEER_PASS;
    String CHARACTEER_NOPASS;*/

    private HashMap<String, Integer> map;

    public Test_2(HashMap<String, Integer> map) {
        this.map = map;
    }

    public void fun() {
        map.put("aa",1);
    }
}
