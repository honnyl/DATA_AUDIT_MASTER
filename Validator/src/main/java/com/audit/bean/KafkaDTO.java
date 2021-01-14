package com.audit.bean;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@Data
public class KafkaDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    private String taskName;
    private String taskId;
    private String historyId;

    private String dbType;
    private String databaseName;
    private String tableName;
    private String taskDescription;
    private DataSourceProperty dataSourceProperty;

    private String hostName;
    private String port;
    private String targetUserName;
    private String targetPassword;
    private String fileName;
    private String fileType;
    private String remotePath;
    private String targetUrl;
    private String targetDriver;
    private String targetSchema;
    private String targetDatabase;

    private List<TaskProperty> rules; //字段规则集合

    /*
    {"HostName": "172.16.2.154",
     "Port": "22",
     "UserName": "root",
     "Password": "qwe123",
     "RemotePath": "/usr/weisc/",
     "FileName":"EMP.csv",
     "FileType":"CSV",
     "TargetID":"b39e38ca-b0b6-1386-ac66-ec7958c62747",
     "TargetDatabase": "default",
     "TargetSchema": "",
     "TargetTableName": "test_weisc",
     "rules":[
            {
           "filedName": "id",
           "standardList": [
            {
              "name": "Date/Time",
                "displayName": "Date/Time",
                "description": "Converts any date to Hive-friendly format with optional timezone conversion",
                "shortDescription": null,
                 "properties": [
                {
                    "name": "Output Format",
                    "displayName": "Output Format",
                    "value": null,
                    "values": null,
                    "placeholder": "",
                    "type": "select",
                    "hint": "Choose an output format",
                    "objectProperty": "outputFormat",
                    "selectableValues": [
                      {
                        "label": "DATE_ONLY",
                        "value": "DATE_ONLY",
                        "hint": null,
                        "properties": null
                      }
                    ],
                    "required": true,
                    "group": "",
                    "groupOrder": 2,
                    "layout": "column",
                    "hidden": false,
                    "pattern": "",
                    "patternInvalidMessage": "Invalid Input",
                    "additionalProperties": []
                      },
                      {
                    "name": "Input timezone",
                    "displayName": "Input timezone",
                    "value": null,
                    "values": null,
                    "placeholder": "",
                    "type": "select",
                    "hint": "Input timezone (optional)",
                    "objectProperty": "inputTimezone",
                    "selectableValues": [
                      {
                        "label": "",
                        "value": "",
                        "hint": null,
                        "properties": null
                      }
                    ],
                    "required": false,
                    "group": "",
                    "groupOrder": 3,
                    "layout": "column",
                    "hidden": false,
                    "pattern": "",
                    "patternInvalidMessage": "Invalid Input",
                    "additionalProperties": []
                  }

                 ]

            }

              ],
              "validateList": [
                {
                    "name": "Regex",
                    "displayName": "Regex",
                    "description": "Validate Regex Pattern",
                    "shortDescription": null,
                    "properties": [
                    {
                           "name": "Regex expression",
                        "displayName": "Regex expression",
                        "value": null,
                        "values": null,
                        "placeholder": "",
                        "type": "string",
                        "hint": "",
                        "objectProperty": "regexExpression",
                        "selectableValues": [],
                        "required": false,
                        "group": "",
                        "groupOrder": 1,
                        "layout": "column",
                        "hidden": false,
                        "pattern": "",
                        "patternInvalidMessage": "Invalid Input",
                        "additionalProperties": []
                    }

                    ],
                    "objectClassType": "com.thinkbiganalytics.validation.RegexValidator",
                    "objectShortClassType": "RegexValidator",
                    "propertyValuesDisplayString": null,
                    "sequence": null
                },
                {
                    "name": "US Zip",
                    "displayName": "US Zip",
                    "description": "Validate US Zip",
                    "shortDescription": null,
                    "properties": [],
                    "objectClassType": "com.thinkbiganalytics.validation.USZipCodeValidator",
                    "objectShortClassType": "USZipCodeValidator",
                    "propertyValuesDisplayString": null,
                    "sequence": null
              }

            ]

        }

         ]
     }
    */

}
