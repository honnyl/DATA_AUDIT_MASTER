package com.audit.validation.test;

import lombok.Data;

import java.util.HashMap;

@Data
public class Test_2 {

    private HashMap<String, Integer> map;

    public Test_2(HashMap<String, Integer> map) {
        this.map = map;
    }

    public void fun() {
        map.put("aa",1);
    }
}
