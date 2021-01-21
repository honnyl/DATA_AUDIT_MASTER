package com.audit.validation.test;

import com.audit.bean.AlterBean;
import com.audit.bean.ValidateCondition;
import com.audit.validation.rule.USZipValidate;
import org.apache.commons.codec.binary.Base64;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class Test {




    public static void main1(String[] args) throws SQLException, ClassNotFoundException {

        Class.forName("oracle.jdbc.driver.OracleDriver");
        java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

        String url = "jdbc:oracle:thin:@//172.18.16.169:1521/orcl";

        Connection conn = DriverManager.getConnection(url, "SCOTT", "tiger");

        String sql = "create table tablename(" + "fieldName VARCHAR(255)," +
                " CreditCard_Pass INT,CreditCard_NoPass INT," +
                " DATE_Pass INT,DATE_NoPass INT," +
                " EMAIL_Pass INT,EMAIL_NoPass INT," +
                " IP_PASS INT,IP_NOPASS INT," +
                " LENGTH_Pass INT,LENGTH_NoPass INT," +
                " LOOKUP_Pass INT,LOOKUP_NoPass INT," +
                " NOTNULL_Pass INT,NOTNULL_NoPass INT," +
                " ONLYONCE_Pass INT,ONLYONCE_NoPass INT," +
                " RANGE_Pass INT,RANGE_NoPass INT," +
                " Regex_Pass INT,Regex_NoPass INT," +
                " TIMESTAMP_Pass INT,TIMESTAMP_NoPass INT," +
                " USPHONE_Pass INT,USPHONE_NoPass INT," +
                " USZIP_Pass INT,USZIP_NoPass INT," +
                " CHARACTEER_Pass INT,CHARACTEER_NoPass INT" +
                ")";



        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.execute();

    }

    public static void main(String[] args) {

        String string = "{\n" +
                "  \"taskId\": \"444556211388416000\",\n" +
                "  \"taskName\": \"MMFULL\",\n" +
                "  \"taskDescription\": null,\n" +
                "  \"historyId\": \"450254512905519104\",\n" +
                "  \"tableName\": \"LHN_LHN\",\n" +
                "  \"targetUserName\": \"root\",\n" +
                "  \"targetPassword\": \"123456\",\n" +
                "  \"targetUrl\": \"jdbc:mysql://172.18.16.207:3306/LHN\",\n" +
                "  \"targetDriver\": \"com.mysql.jdbc.Driver\",\n" +
                "  \"targetDatabase\": null,\n" +
                "  \"targetSchema\": \"LHN\",\n" +
                "  \"dbType\": \"MySQL\",\n" +
                "  \"dataSourceProperty\": {\n" +
                "    \"schema\": null,\n" +
                "    \"user\": \"root\",\n" +
                "    \"password\": \"123456\",\n" +
                "    \"driver\": \"com.mysql.jdbc.Driver\",\n" +
                "    \"url\": \"jdbc:mysql://172.18.16.207:3306/LHN\"\n" +
                "  },\n" +
                "  \"rules\": [\n" +
                "    {\n" +
                "      \"fieldName\": \"ID\",\n" +
                "      \"fieldType\": \"DECIMAL\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": [\n" +
                "        {\n" +
                "          \"name\": \"长度\",\n" +
                "          \"displayName\": null,\n" +
                "          \"description\": null,\n" +
                "          \"shortDescription\": null,\n" +
                "          \"properties\": [\n" +
                "            {\n" +
                "              \"name\": \"最大长度\",\n" +
                "              \"displayName\": null,\n" +
                "              \"value\": \"4\",\n" +
                "              \"subValue1\": null,\n" +
                "              \"subValue2\": null,\n" +
                "              \"values\": null,\n" +
                "              \"placeholder\": null,\n" +
                "              \"type\": null,\n" +
                "              \"hint\": null,\n" +
                "              \"objectProperty\": \"maxLength\",\n" +
                "              \"selectableValues\": [\n" +
                "                \n" +
                "              ],\n" +
                "              \"required\": false,\n" +
                "              \"group\": null,\n" +
                "              \"groupOrder\": null,\n" +
                "              \"layout\": \"column\",\n" +
                "              \"hidden\": false,\n" +
                "              \"pattern\": null,\n" +
                "              \"patternInvalidMessage\": null,\n" +
                "              \"additionalProperties\": [\n" +
                "                \n" +
                "              ]\n" +
                "            },\n" +
                "            {\n" +
                "              \"name\": \"最小长度\",\n" +
                "              \"displayName\": null,\n" +
                "              \"value\": \"4\",\n" +
                "              \"subValue1\": null,\n" +
                "              \"subValue2\": null,\n" +
                "              \"values\": null,\n" +
                "              \"placeholder\": null,\n" +
                "              \"type\": null,\n" +
                "              \"hint\": null,\n" +
                "              \"objectProperty\": \"minLength\",\n" +
                "              \"selectableValues\": [\n" +
                "                \n" +
                "              ],\n" +
                "              \"required\": false,\n" +
                "              \"group\": null,\n" +
                "              \"groupOrder\": null,\n" +
                "              \"layout\": \"column\",\n" +
                "              \"hidden\": false,\n" +
                "              \"pattern\": null,\n" +
                "              \"patternInvalidMessage\": null,\n" +
                "              \"additionalProperties\": [\n" +
                "                \n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"objectClassType\": null,\n" +
                "          \"objectShortClassType\": null,\n" +
                "          \"propertyValuesDisplayString\": null,\n" +
                "          \"sequence\": null\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"NAME\",\n" +
                "      \"fieldType\": \"VARCHAR\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": [\n" +
                "        {\n" +
                "          \"name\": \"非空\",\n" +
                "          \"displayName\": null,\n" +
                "          \"description\": null,\n" +
                "          \"shortDescription\": null,\n" +
                "          \"properties\": [\n" +
                "            \n" +
                "          ],\n" +
                "          \"objectClassType\": null,\n" +
                "          \"objectShortClassType\": null,\n" +
                "          \"propertyValuesDisplayString\": null,\n" +
                "          \"sequence\": null\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"AGE\",\n" +
                "      \"fieldType\": \"DECIMAL\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": null\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"PHONE\",\n" +
                "      \"fieldType\": \"VARCHAR\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": [\n" +
                "        {\n" +
                "          \"name\": \"电话号码\",\n" +
                "          \"displayName\": null,\n" +
                "          \"description\": null,\n" +
                "          \"shortDescription\": null,\n" +
                "          \"properties\": [\n" +
                "            \n" +
                "          ],\n" +
                "          \"objectClassType\": null,\n" +
                "          \"objectShortClassType\": null,\n" +
                "          \"propertyValuesDisplayString\": null,\n" +
                "          \"sequence\": null\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"EAMIL\",\n" +
                "      \"fieldType\": \"VARCHAR\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": null\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"IP\",\n" +
                "      \"fieldType\": \"VARCHAR\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": null\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"ZIPCODE\",\n" +
                "      \"fieldType\": \"VARCHAR\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": null\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"SAL\",\n" +
                "      \"fieldType\": \"DECIMAL\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": null\n" +
                "    },\n" +
                "    {\n" +
                "      \"fieldName\": \"CHARSS\",\n" +
                "      \"fieldType\": \"VARCHAR\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": null\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        System.out.println(string);
    }

}
