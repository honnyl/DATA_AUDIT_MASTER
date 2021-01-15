package com.audit.entry;

import com.alibaba.fastjson.JSON;
import com.audit.bean.*;
import com.audit.rowoperate.*;
import com.audit.validation.rule.OnlyOnceValidateRule;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.PrintStream;
import java.sql.*;
import java.util.*;

public class RunEntry {

    private static final Logger log = LoggerFactory.getLogger(RunEntry.class);
    private static final String hiveUrl = "thrift://172.18.16.208:9083";

    public static void main(String[] args) {

        if (log.isInfoEnabled()) {
            log.info("Running Spark RunValidatorLocal with the following command line args (comma separated):{}", org.apache.commons.lang.StringUtils.join(args, ","));
        }
        try {
            new RunEntry().run(System.out, args);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            // The validator will have created the context being stopped.
            SparkContext.getOrCreate().stop();
        }
    }

    private void run(final PrintStream out, final String... args) throws ClassNotFoundException, SQLException {

        String param = args[0];

        KafkaDTO kafkaDTO = JSON.parseObject(param, KafkaDTO.class);

        /*SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("hive.metastore.uris", hiveUrl)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .enableHiveSupport()
                .getOrCreate();*/

        String tableName = kafkaDTO.getTargetSchema() + "." + kafkaDTO.getTableName();
        String taskId = kafkaDTO.getTaskId();
        String historyId = kafkaDTO.getHistoryId();

        Dataset<Row> sqlData = null;
        SparkSession spark = null;

        if (kafkaDTO.getDbType().toUpperCase().equals("HIVE")) {
            tableName = kafkaDTO.getTargetDatabase() + "." + kafkaDTO.getTableName();
        }

        System.out.println("tableName======" + tableName);
        if (kafkaDTO.getDbType().toUpperCase().equals("HIVE")) {
            String warehouseLocation = new File("/warehouse/tablespace/managed/hive").getAbsolutePath();
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            spark = SparkSession
                    .builder()
                    .config("spark.sql.warehouse.dir", warehouseLocation)
                    .config("hive.metastore.uris", hiveUrl)
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .enableHiveSupport()
                    .getOrCreate();
            sqlData = spark.table(tableName);

        } else {
            spark = SparkSession
                    .builder()
                    .config(new SparkConf().setAppName("TEST"))
                    .getOrCreate();

            String userName = kafkaDTO.getTargetUserName();
            Properties properties = new Properties();
            properties.put("user", userName);
            properties.put("password",kafkaDTO.getTargetPassword());
            properties.put("driver",kafkaDTO.getTargetDriver());
            sqlData = spark.read().jdbc(kafkaDTO.getTargetUrl(), tableName, properties);
            sqlData.show();
        }

        if (sqlData != null && spark != null) {

            List<TaskProperty> rules = kafkaDTO.getRules();
            StructType fields = sqlData.schema();
            List<Tuple2<String, String>> fieldNames = new ArrayList<>();
            ArrayList<String> field = new ArrayList<>();

            List<Tuple2<String, List<ValidateCondition>>> validateConditionListS = new ArrayList<Tuple2<String, List<ValidateCondition>>>();
            List<Tuple2<String, List<ValidateCondition>>> validateConditionListV = new ArrayList<Tuple2<String, List<ValidateCondition>>>();



            Class.forName("oracle.jdbc.driver.OracleDriver");
            java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            String url = "jdbc:oracle:thin:@//172.18.16.169:1521/orcl";
            Connection conn = DriverManager.getConnection(url, "SCOTT", "tiger");

            PreparedStatement stmt = null;

            //得出需要校验的字段和检验名称
            for (TaskProperty rule : rules) {
                String uuid = UUID.randomUUID().toString();
                fieldNames.add(new Tuple2<String, String>(uuid, rule.getFieldName()));
                field.add(rule.getFieldName());
                if (rule.getStandardList() != null) {
                    validateConditionListS.add(new Tuple2<>(uuid, rule.getStandardList()));
                }

                if (rule.getValidateList() != null) {
                    prepareFieldValidatedTable(field,tableName,taskId, historyId);
                    for (ValidateCondition validateCondition : rule.getValidateList()) {
                        if (validateCondition.getName().equals("查询")) {
                            OnlyOnceValidateRule.duplicatedFields(kafkaDTO, rule.getFieldName());
                        }

                        String validate_name = "";

                        if (validateCondition.getName().equals("查询") || validateCondition.getName().equals("字符")) {

                            validate_name = validateCondition.getProperties().get(0).getValue();
                        } else validate_name =  validateCondition.getName();
                        String sql = "insert into " + tableName + "_FIELD_VALIDATED " +
                                "values ( \'" + taskId + "\',\'" + historyId + "\', \'"
                                + rule.getFieldName() + "\', \'" + validate_name +  "\',"
                                + "0,0 )";
                        stmt = conn.prepareStatement(sql);
                        stmt.execute();
                    }
                    validateConditionListV.add(new Tuple2<>(uuid, rule.getValidateList()));
                }
            }


            if (validateConditionListS.size() != 0) {
                operateRowStandard(rules, spark, fieldNames, validateConditionListS, sqlData, taskId, fields, kafkaDTO);
            }

            if (validateConditionListV.size() != 0) {

                try {
                    rowOperateValidate(rules, spark, fieldNames, validateConditionListV, sqlData, taskId, fields, kafkaDTO, tableName, field, historyId);
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void operateRowStandard(List<TaskProperty> rules, SparkSession spark,List<Tuple2<String,String>> fieldNames,
                                          List<Tuple2<String,List<ValidateCondition>>> validateConditionList, Dataset<Row> sqlData,
                                          String taskId,StructType fields,KafkaDTO kafkaDTO ) {


        final CleanseAndAlterRow functionAlter = new CleanseAndAlterRow(validateConditionList, taskId, fields, fieldNames);
        sqlData.show();
        JavaRDD<Row> map2 = sqlData.javaRDD().map(functionAlter);
        Dataset<Row> dataFrameAlter = spark.createDataFrame(map2, fields);

        dataFrameAlter.show();
        wirteToHive(dataFrameAlter, kafkaDTO);
        wirteToTargetTable(dataFrameAlter, kafkaDTO, "_ALTERED");
    }




    private static void rowOperateValidate(List<TaskProperty> rules, SparkSession spark, List<Tuple2<String,String>> fieldNames,
                                           List<Tuple2<String,List<ValidateCondition>>> validateConditionList, Dataset<Row> sqlData,
                                           String taskId, StructType fields, KafkaDTO kafkaDTO, String tableName, List<String> field, String historyId) throws SQLException, ClassNotFoundException {

        Dataset<Row> dataFrame = null;
        StructType add1 = null;


        final CleanseAndValidateRow function = new CleanseAndValidateRow(validateConditionList, taskId, fields, fieldNames,tableName, historyId);
        JavaRDD<Row> map1 = sqlData.javaRDD().map(function);

        add1 = fields.add("invalidate_error".toUpperCase(), "String");
        add1 = add1.add("task_id".toUpperCase(), "String");
        add1 = add1.add("sequence_key".toUpperCase(), "String");
        add1 = add1.add("history_id".toUpperCase(), "String");

        dataFrame = spark.createDataFrame(map1, add1);
        //dataFrame.show();
        wirteToOracle(dataFrame, kafkaDTO, "_VALIDATED");
    }



    private static void wirteToTargetTable(Dataset<Row> dataFrame,  KafkaDTO kafkaDTO, String flag) {
        Properties properties = new Properties();

        properties.setProperty("user", kafkaDTO.getTargetUserName());
        properties.setProperty("password", kafkaDTO.getTargetPassword());
        properties.setProperty("batchsize", "100");
        properties.setProperty("truncate", "true");
        properties.setProperty("driver", kafkaDTO.getTargetPassword());
        dataFrame.write().mode("overwrite").jdbc(kafkaDTO.getDataSourceProperty().getUrl(), kafkaDTO.getDataSourceProperty().getSchema() + "." + kafkaDTO.getTableName().toUpperCase() + flag, properties);

    }


    private static void wirteToOracle(Dataset<Row> dataFrame,  KafkaDTO kafkaDTO, String flag) {
        Properties properties = new Properties();

        properties.setProperty("user", "SCOTT");
        properties.setProperty("password", "tiger");
        properties.setProperty("batchsize", "100");
        properties.setProperty("truncate", "true");
        properties.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
        dataFrame
                .write()
                .mode("append")
                .jdbc("jdbc:oracle:thin:@//172.18.16.169:1521/orcl", "SCOTT" + "." + kafkaDTO.getTableName().toUpperCase() + flag, properties);

    }


    private static void wirteToHive(Dataset<Row> dataFrameAlter , KafkaDTO kafkaDTO) {

        String warehouseLocation = new File("/warehouse/tablespace/managed/hive").getAbsolutePath();
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("hive.metastore.uris", hiveUrl)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .enableHiveSupport()
                .getOrCreate();


        //将修改后的数据存入hive中

        String newTable = kafkaDTO.getTableName() + "_altered";
        spark.sql("drop table if exists " + newTable);
        spark.sql("create table if not exists " + newTable + " like " + kafkaDTO.getDatabaseName() + "." + kafkaDTO.getTableName());

        dataFrameAlter.createOrReplaceTempView(kafkaDTO.getTableName() + "_temp");
        spark.sql("insert overwrite table " + newTable + " select * from " + kafkaDTO.getTableName() + "_temp");

        spark.sql("select * from " + newTable + " limit 10").show();

    }


    private static void prepareFieldValidatedTable(List<String> fields, String tableName, String taskId, String historyId) throws ClassNotFoundException, SQLException {

        Class.forName("oracle.jdbc.driver.OracleDriver");
        java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

        String url = "jdbc:oracle:thin:@//172.18.16.169:1521/orcl";

        Connection conn = DriverManager.getConnection(url, "SCOTT", "tiger");
        PreparedStatement stmt = null;

        tableName = tableName.toUpperCase() + "_FIELD_VALIDATED";



        String sql = ("select count(1) as num from USER_TABLES where table_name='" + tableName.split("\\.")[1] + "'").toUpperCase();

        stmt = conn.prepareStatement(sql);
        ResultSet countResult = stmt.executeQuery();
        while (countResult.next()) {
            int resultInt = countResult.getInt(1);

            if (resultInt == 0) {

                sql = "create table " + tableName  + "(" +
                        "TASK_ID VARCHAR(255)," +
                        "HISTORY_ID VARCHAR(255)," +
                        "FIELDNAME VARCHAR(255)," +
                        "VALIDATE_TYPE VARCHAR(255)," +
                        "PASS INT," +
                        "PASS_NO INT" +
                        ")".toUpperCase();

                stmt = conn.prepareStatement(sql);
                stmt.execute();

            } else {
                return;
            }

        }



        /*for (String field : fields) {
            sql = "insert into " + tableName + " values (\'" + field + "\' ,\'" + taskId + "\' ,\'" + historyId + "\', null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)";
            stmt = conn.prepareStatement(sql);
            stmt.execute();
        }*/
    }

}
