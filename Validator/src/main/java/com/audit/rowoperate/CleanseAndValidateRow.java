package com.audit.rowoperate;


import com.audit.validation.rule.*;
import lombok.Data;
import org.apache.calcite.plan.Strong;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

import com.audit.bean.ValidateCondition;
import scala.Int;
import scala.Tuple2;

import static com.audit.utils.ConstantUtil.*;
//KafkaDTO--->TaskProperty----->ValidateCondition----->ValidateProperty---->ValidateValues/AdditionalProperties

/**
 * @Description: spark的函数
 * @Author: weisc
 * @CreateDate: 2020/5/27 14:32
 * @Version: 1.0
 */

public class CleanseAndValidateRow implements Function<Row, Row> {

    private StructType fields;
    private String[] fieldNames;
    private String taskId;
    private List<Tuple2<String,List<ValidateCondition>>> validateConditionList;
    private List<Tuple2<String,String>> field;
    private static final Logger log = LoggerFactory.getLogger(CleanseAndValidateRow.class);
    private ArrayList<String> error;
    private String tableName;
    private String historyId;

    public CleanseAndValidateRow(List<Tuple2<String,List<ValidateCondition>>> validateConditionList, String taskId, StructType fields,
                                 List<Tuple2<String,String>> field,String tableName, String historyId) {
        this.fields = fields;
        this.fieldNames = fields.fieldNames();
        this.taskId = taskId;
        this.validateConditionList = validateConditionList;
        this.field = field;
        this.tableName = tableName;
        this.historyId = historyId;
    }

    /***\
     * 实现方法，数据每行传入，进行相应的加工处理
     * @param row
     * @return
     * @throws Exception
     */
    @Override
    public Row call(Row row) throws Exception {

        PreparedStatement stmt = null;

        Class.forName("oracle.jdbc.driver.OracleDriver");
        java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        String url = "jdbc:oracle:thin:@//172.18.16.169:1521/orcl";
        Connection conn = DriverManager.getConnection(url, "SCOTT", "tiger");

        String strTmp = "";

        Object[] newValues = new Object[fieldNames.length + 4];

        for (int i = 0; i < row.length(); i++) {
            newValues[i] = (row.isNullAt(i) ? null : row.get(i));
        }

        String invalidateError = "";

        for (Tuple2<String,String> fieldTuple : field ) {

            boolean flag = true;

            for (Tuple2<String,List<ValidateCondition>> vcl : validateConditionList) {

                for (ValidateCondition vc : vcl._2) {
                    //获取稽核名称：regex
                    String validateName = vc.getName();

                    if (fieldTuple._1.equals(vcl._1)) {

                            for (int idx = 0; idx < fieldNames.length; idx++) {
                                Object fieldValue = (row.isNullAt(idx) ? null : row.get(idx));
                                newValues[idx] = fieldValue;

                                if (fieldNames[idx].toUpperCase().equals(fieldTuple._2.toUpperCase())) {

                                    if (vc != null) {
                                        String errorString = execValidateRule(fieldValue, fieldNames[idx], vc, validateName);

                                        //=======================================


                                        if (vc.getName().equals("查询") || vc.getName().equals("字符")) {
                                            validateName = vc.getProperties().get(0).getValue();
                                        }

                                        int num = 0;

                                        if (StringUtils.isNotEmpty(errorString)) {

                                            flag = false;

                                            String sql = "select PASS_NO from "+ tableName + "_FIELD_VALIDATED " +  " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                                                    " and HISTORY_ID = '" + historyId + "'" +
                                                    " and VALIDATE_TYPE = '" + validateName + "'".toUpperCase();

                                            stmt = conn.prepareStatement(sql);
                                            ResultSet countResult = stmt.executeQuery(sql);
                                            if (countResult != null) {
                                                while (countResult.next()) {
                                                    Object result = countResult.getObject(1);
                                                    if (result == null) {
                                                        num = 0;
                                                    } else {
                                                        num = Integer.parseInt(result.toString());
                                                    }
                                                }
                                            }


                                            num ++;
                                            System.out.println("passNo-------" + validateName + num);
                                            sql = "update " + tableName + "_FIELD_VALIDATED " +
                                                    "set PASS_NO = " + num +
                                                    " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                                                    " and HISTORY_ID = '" + historyId + "'" +
                                                    " and VALIDATE_TYPE = '" + validateName + "'".toUpperCase();
                                            stmt = conn.prepareStatement(sql);
                                            stmt.execute();
                                        } else {

                                            String sql = "select PASS from "+ tableName + "_FIELD_VALIDATED " +  " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                                                    " and HISTORY_ID = '" + historyId + "'" +
                                                    " and VALIDATE_TYPE = '" + validateName + "'".toUpperCase();
                                            stmt = conn.prepareStatement(sql);
                                            ResultSet countResult = stmt.executeQuery(sql);
                                            if (countResult != null) {
                                                while (countResult.next()) {
                                                    Object result = countResult.getObject(1);
                                                    if (result == null) {
                                                        num = 0;
                                                    } else {
                                                        num = Integer.parseInt(result.toString());
                                                    }
                                                }
                                            }

                                            num++;
                                            System.out.println("pass-------" + validateName + num);
                                            sql = "update " + tableName + "_FIELD_VALIDATED " +
                                                    "set PASS = " + num +
                                                    " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                                                    " and HISTORY_ID = '" + historyId + "'" +
                                                    " and VALIDATE_TYPE = '" + validateName + "'".toUpperCase();
                                            stmt = conn.prepareStatement(sql);
                                            stmt.execute();
                                        }

                                        //================================================


                                        if (StringUtils.isNotEmpty(invalidateError) && StringUtils.isNotEmpty(errorString)) {
                                            invalidateError = invalidateError + errorString;
                                        } else if (StringUtils.isNotEmpty(invalidateError) && !StringUtils.isNotEmpty(errorString)) {
                                            invalidateError = invalidateError;
                                        } else {
                                            invalidateError = errorString;
                                        }

                                    } else {
                                        throw new NullPointerException("The ValidateCondition is null");
                                    }
                                }
                            }

                            /*PreparedStatement stmt = null;

                            passRate = passNum / totalCount;
                            noPassRate = noPassNum / totalCount;
                            String sql = "insert into " + tableName + "_FIELD_VALIDATED " +
                                    "values ( \'" + taskId + "\',\'" + historyId + "\', \'" + fieldTuple._2.toUpperCase() + "\', \'" + validateName +  "\'," + passNum + "," + noPassNum + "," + passRate + "," + noPassRate + ")";
                            stmt = conn.prepareStatement(sql);
                            stmt.execute();
                            row = RowFactory.create(newValues);*/

                        }

                    newValues[fieldNames.length] = invalidateError;
                    newValues[fieldNames.length + 1] = taskId;
                    newValues[fieldNames.length + 2] = UUID.randomUUID().toString();
                    newValues[fieldNames.length + 3] = historyId;


                    row = RowFactory.create(newValues);
                }

            }

            int num = 0;

            if (flag) {

                String sql = "select PASS from "+ tableName + "_FIELD_SUMMARY " +  " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                        " and HISTORY_ID = '" + historyId + "'" +
                        " and TASK_ID = '" + taskId + "'".toUpperCase();
                stmt = conn.prepareStatement(sql);
                ResultSet countResult = stmt.executeQuery(sql);
                if (countResult != null) {
                    while (countResult.next()) {
                        Object result = countResult.getObject(1);
                        num = Integer.parseInt(result.toString());
                    }
                }
                num++;

                sql = "update " + tableName + "_FIELD_SUMMARY " +
                        "set PASS = " + num +
                        " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                        " and HISTORY_ID = '" + historyId + "'" +
                        " and TASK_ID = '" + taskId + "'".toUpperCase();
                stmt = conn.prepareStatement(sql);
                stmt.execute();


            } else {
                String sql = "select PASS_NO from "+ tableName + "_FIELD_SUMMARY " +  " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                        " and HISTORY_ID = '" + historyId + "'" +
                        " and TASK_ID = '" + taskId + "'".toUpperCase();
                stmt = conn.prepareStatement(sql);
                ResultSet countResult = stmt.executeQuery(sql);
                if (countResult != null) {
                    while (countResult.next()) {
                        Object result = countResult.getObject(1);
                        num = Integer.parseInt(result.toString());
                    }
                }
                num ++;
                sql = "update " + tableName + "_FIELD_SUMMARY " +
                        "set PASS_NO = " + num +
                        " where  FIELDNAME = '" + fieldTuple._2 + "'" +
                        " and HISTORY_ID = '" + historyId + "'" +
                        " and TASK_ID = '" + taskId + "'".toUpperCase();
                stmt = conn.prepareStatement(sql);
                stmt.execute();

            }

        }

        stmt.close();
        conn.close();

        return row;
    }

    /***
     * 执行规则
     * @param val
     * @param validateCondition
     * @return
     */
    public String execValidateRule(Object val, String fieldName, ValidateCondition validateCondition, String typeName) {
        String value;
        ValidateRule validateRule = getRule(typeName);
        value = validateRule.validateRowValue(val, fieldName, validateCondition).getInvalidateError();

        return value;
    }

    /***
     * 得到相对应的规则
     * @param typeName
     * @return
     */
    public ValidateRule getRule(String typeName) {
        ValidateRule validateRule = null;
        switch (typeName) {
            case DATE:
                validateRule = new DateValidate();
                break;
            case LENGTH:
                validateRule = new LengthValidate();
                break;
            case CREDITCARD:
                validateRule = new CreditCardValidate();
                break;
            case EMAIL:
                validateRule = new EmailValidate();
                break;
            case IPADDRESS:
                validateRule = new IPAddressValidate();
                break;
            case LOOKUP:
                validateRule = new LookupValidate();
                break;
            case NOTNULL:
                validateRule = new NotNullValidate();
                break;
            case RANGE:
                validateRule = new RangeValidate();
                break;
            case REGEX:
                validateRule = new RegexValidate();
                break;
            case TIMESTAMP:
                validateRule = new TimestampValidate();
                break;
            case USPHONE:
                validateRule = new USPhoneValidate();
                break;
            case USZIP:
                validateRule = new USZipValidate();
                break;
            case VALIDATECHARACTERS:
                validateRule = new ValidateCharactersValidate();
                break;

            default:
                log.error("没有对应的稽核规则！");
        }

        return validateRule;
    }


    public String confirmFields(String field) {

        String flag = "";

        switch (field) {
            case DATE:
                flag = "DATE_PASS";
                break;
            case LENGTH:
                flag = "LENGTH_PASS";
                break;
            case CREDITCARD:
                flag = "CREDITCARD_PASS";
                break;
            case EMAIL:
                flag = "EMAIL_PASS";
                break;
            case IPADDRESS:
                flag = "IP_PASS";
                break;
            case LOOKUP:
                flag = "LOOKUP_PASS";
                break;
            case NOTNULL:
                flag = "NOTNULL_PASS";
                break;
            case RANGE:
                flag = "RANGE_PASS";
                break;
            case REGEX:
                flag = "REGEX_PASS";
                break;
            case TIMESTAMP:
                flag = "TIMESTAP_PASS";
                break;
            case USPHONE:
                flag = "PHONE_PASS";
                break;
            case USZIP:
                flag = "ZIP_PASS";
                break;
            case VALIDATECHARACTERS:
                flag = "CHAR_PASS";
                break;

            default:
                log.error("属性名错误！");
        }
        return flag;
    }
}
