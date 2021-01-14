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

        HashMap<String, Integer> map = new HashMap<>();
        Test_2 test_2 = new Test_2(map);
        test_2.fun();
        map.get("aa");
    }

}
