package com.audit.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * @Description: java类作用描述
 * @Author: weisc
 * @CreateDate: 2020/6/4 16:54
 * @Version: 1.0
 */
@Data
public class DataSourceProperty implements Serializable {
    private static final long serialVersionUID = 1L;
    private String user;
    private String password;
    private String driver;
    private String url;
    private String schema;

}
