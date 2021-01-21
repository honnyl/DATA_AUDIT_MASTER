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

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        if (log.isInfoEnabled()) {
            log.info("Running Spark RunValidatorLocal with the following command line args (comma separated):{}", org.apache.commons.lang.StringUtils.join(args, ","));
        }
            new RunEntry().run(System.out, args);

            // The validator will have created the context being stopped.
            SparkContext.getOrCreate().stop();

    }

    private void run(final PrintStream out, final String... args) throws ClassNotFoundException, SQLException {

        String param = "{\n" +
                "  \"taskId\": \"444556211388416000\",\n" +
                "  \"taskName\": \"MMFULL\",\n" +
                "  \"taskDescription\": null,\n" +
                "  \"historyId\": \"450254512905519104\",\n" +
                "  \"tableName\": \"PPPPPP\",\n" +
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
                "      \"fieldName\": \"CHARSS\",\n" +
                "      \"fieldType\": \"VARCHAR\",\n" +
                "      \"standardList\": null,\n" +
                "      \"validateList\": [\n" +
                "        {\n" +
                "          \"name\": \"查询\",\n" +
                "          \"displayName\": null,\n" +
                "          \"description\": null,\n" +
                "          \"shortDescription\": null,\n" +
                "          \"properties\": [\n" +
                "            {\n" +
                "              \"name\": \"唯一性检核\",\n" +
                "              \"displayName\": null,\n" +
                "              \"value\": \"唯一性检核\"\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        KafkaDTO kafkaDTO = JSON.parseObject(param, KafkaDTO.class);

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
            spark = getSparkSession();
            sqlData = spark.table(tableName);

        } else {
            spark = SparkSession
                    .builder()
                    .master("local[*]")
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

            Connection conn = getOracleConn();

            PreparedStatement stmt = null;
            String sql = "";

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
                    fieldSummaryTable(field,tableName,taskId, historyId);


                    for (ValidateCondition validateCondition : rule.getValidateList()) {
                        if (validateCondition.getName().equals("查询")) {
                            OnlyOnceValidateRule.duplicatedFields(kafkaDTO, rule.getFieldName());
                        }

                        String validate_name = "";

                        if (validateCondition.getName().equals("查询") || validateCondition.getName().equals("字符")) {

                            validate_name = validateCondition.getProperties().get(0).getValue();
                        } else validate_name =  validateCondition.getName();

                        //字段稽核表
                        sql = "insert into " + tableName.split("\\.")[1] + "_FIELD_VALIDATED " +
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

            for (TaskProperty rule : rules) {
                //字段汇总表
                sql = "insert into " + tableName.split("\\.")[1] + "_FIELD_SUMMARY " +
                        "values ( \'" + taskId + "\',\'" + historyId + "\', \'"
                        + rule.getFieldName() + "\', "
                        + "0,0,0,0,0 )";
                stmt = conn.prepareStatement(sql);
                stmt.execute();
            }

            if (validateConditionListV.size() != 0) {
                rowOperateValidate(rules, spark, fieldNames, validateConditionListV, sqlData, taskId, fields, kafkaDTO, tableName.split("\\.")[1], field, historyId);
                updateFieldSummaryTable(field,tableName,taskId,historyId);
            }

            stmt.close();
            conn.close();

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

        SparkSession spark = SparkSession.builder().getOrCreate();

        Properties properties = new Properties();

        properties.setProperty("user", "SCOTT");
        properties.setProperty("password", "tiger");
        properties.setProperty("batchsize", "1000");
        properties.setProperty("truncate", "true");
        properties.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
        dataFrame.show();

        dataFrame
                .write()
                .mode("overwrite")
                .jdbc("jdbc:oracle:thin:@//172.18.16.169:1521/orcl", "SCOTT" + "." + kafkaDTO.getTableName().toUpperCase() + flag, properties);



    }


    private static void wirteToHive(Dataset<Row> dataFrameAlter , KafkaDTO kafkaDTO) {

        SparkSession spark = getSparkSession();

        //将修改后的数据存入hive中

        String newTable = "lhn." + kafkaDTO.getTableName() + "_altered";
        spark.sql("drop table if exists " + newTable);
        spark.sql("create table if not exists " + newTable + " like " + "lhn"+ "." + kafkaDTO.getTableName());

        dataFrameAlter.createOrReplaceTempView(kafkaDTO.getTableName() + "_temp");
        spark.sql("insert overwrite table " + newTable + " select * from " + kafkaDTO.getTableName() + "_temp");

        spark.sql("select * from " + newTable + " limit 10").show();

    }


    private static void prepareFieldValidatedTable(List<String> fields, String tableName, String taskId, String historyId) throws ClassNotFoundException, SQLException {

        Connection conn = getOracleConn();
        PreparedStatement stmt = null;

        tableName = tableName.toUpperCase() + "_FIELD_VALIDATED";



        String sql = ("select count(1) as num from USER_TABLES where table_name='" + tableName.split("\\.")[1] + "'").toUpperCase();

        stmt = conn.prepareStatement(sql);
        ResultSet countResult = stmt.executeQuery();
        while (countResult.next()) {
            int resultInt = countResult.getInt(1);

            if (resultInt == 0) {

                sql = "create table " + tableName.split("\\.")[1] + "(" +
                        "TASK_ID VARCHAR(255)," +
                        "HISTORY_ID VARCHAR(255)," +
                        "FIELDNAME VARCHAR(255)," +
                        "VALIDATE_TYPE VARCHAR(255)," +
                        "PASS INT," +
                        "PASS_NO INT" +
                        ")".toUpperCase();

                stmt = conn.prepareStatement(sql);
                stmt.execute();

                stmt.close();
                conn.close();

            } else {
                return;
            }

        }
    }





    private static void fieldSummaryTable(List<String> fields, String tableName, String taskId, String historyId) throws ClassNotFoundException, SQLException {

        Connection conn = getOracleConn();

        PreparedStatement stmt = null;

        tableName = tableName.toUpperCase() + "_FIELD_SUMMARY";

        String sql = ("select count(1) as num from USER_TABLES where table_name='" + tableName.split("\\.")[1] + "'").toUpperCase();

        stmt = conn.prepareStatement(sql);
        ResultSet countResult = stmt.executeQuery();
        while (countResult.next()) {
            int resultInt = countResult.getInt(1);

            if (resultInt == 0) {

                sql = "create table " + tableName.split("\\.")[1] + "(" +
                        "TASK_ID VARCHAR(255)," +
                        "HISTORY_ID VARCHAR(255)," +
                        "FIELDNAME VARCHAR(255)," +
                        "PASS INT," +
                        "PASS_NO INT," +
                        "DUPLICATE_VALUE INT, " +
                        "UNIQUE_VALUE INT, " +
                        "NULL_VALUE INT" +
                        ")".toUpperCase();

                stmt = conn.prepareStatement(sql);
                stmt.execute();
                stmt.close();
                conn.close();

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


    private static Connection getOracleConn() throws ClassNotFoundException, SQLException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        java.sql.DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

        String url = "jdbc:oracle:thin:@//172.18.16.169:1521/orcl";

        Connection conn = DriverManager.getConnection(url, "SCOTT", "tiger");

        return conn;
    }

    public static SparkSession getSparkSession() {
        String warehouseLocation = new File("/warehouse/tablespace/managed/hive").getAbsolutePath();
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("hive.metastore.uris", hiveUrl)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .enableHiveSupport()
                .getOrCreate();

        return spark;
    }


    private static void updateFieldSummaryTable(List<String> fields, String tableName, String taskId, String historyId) throws SQLException, ClassNotFoundException {
        Connection conn = getOracleConn();
        String sql = "";
        PreparedStatement stmt = null;

        tableName = tableName.split("\\.")[1];

        for (String field : fields) {
            sql = "select count(*) from " + "SCOTT." + tableName + "_VALIDATED where " +
                    " HISTORY_ID = '" + historyId + "'" +
                    " and TASK_ID = '" + taskId + "'" +
                    " and " + field + " is not null" +
                    " group by " + field +
                    " having count(*) = 1";

            stmt = conn.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery(sql);
            int count = 0;

            while (resultSet.next()) {
                count += Integer.parseInt(resultSet.getObject(1).toString());
            }
            sql = "update " + tableName + "_FIELD_SUMMARY " +
                    "set UNIQUE_VALUE = " + count +
                    " where  FIELDNAME = '" + field + "'" +
                    " and HISTORY_ID = '" + historyId + "'" +
                    " and TASK_ID = '" + taskId + "'";
            stmt = conn.prepareStatement(sql);
            stmt.execute();

            //------------------------重复值---------------------------

            sql = "select count(*) from " + "SCOTT." + tableName + "_VALIDATED where " +
                    " HISTORY_ID = '" + historyId + "'" +
                    " and TASK_ID = '" + taskId + "'" +
                    " and " + field + " is not null" +
                    " group by " + field +
                    " having count(*) > 1";

            stmt = conn.prepareStatement(sql);
            resultSet = stmt.executeQuery(sql);
            count = 0;

            while (resultSet.next()) {
                count += Integer.parseInt(resultSet.getObject(1).toString());
            }
            sql = "update " + tableName + "_FIELD_SUMMARY " +
                    "set DUPLICATE_VALUE = " + count +
                    " where  FIELDNAME = '" + field + "'" +
                    " and HISTORY_ID = '" + historyId + "'" +
                    " and TASK_ID = '" + taskId + "'";
            stmt = conn.prepareStatement(sql);
            stmt.execute();


            //------------------------空值---------------------------
            sql = "select count(*) from " + "SCOTT." + tableName + "_VALIDATED where " +
                    " HISTORY_ID = '" + historyId + "'" +
                    " and TASK_ID = '" + taskId + "'" +
                    " and " + field + " is null";

            stmt = conn.prepareStatement(sql);
            resultSet = stmt.executeQuery(sql);
            count = 0;

            while (resultSet.next()) {
                count += Integer.parseInt(resultSet.getObject(1).toString());
            }
            sql = "update " + tableName + "_FIELD_SUMMARY " +
                    "set NULL_VALUE= " + count +
                    " where  FIELDNAME = '" + field + "'" +
                    " and HISTORY_ID = '" + historyId + "'" +
                    " and TASK_ID = '" + taskId + "'";
            stmt = conn.prepareStatement(sql);
            stmt.execute();

            resultSet.close();
            stmt.close();
            conn.close();
        }
    }




}
