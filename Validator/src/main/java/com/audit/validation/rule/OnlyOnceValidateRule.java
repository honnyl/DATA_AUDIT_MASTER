package com.audit.validation.rule;

import com.audit.bean.DataSourceProperty;
import com.audit.bean.KafkaDTO;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.audit.entry.RunEntry.getSparkSession;

public class OnlyOnceValidateRule {

    public static void duplicatedFields(KafkaDTO kafkaDTO, String fieldName) {
        String tableName = kafkaDTO.getTableName();
        SparkSession spark = SparkSession.builder().getOrCreate();

        if (kafkaDTO.getDbType().toUpperCase().equals("HIVE")) {

            spark.table(tableName).createOrReplaceGlobalTempView("OnlyOnceValidate_" + tableName);

        } else {

            String userName = kafkaDTO.getTargetUserName();
            Properties properties = new Properties();
            properties.put("user", userName);
            properties.put("password",kafkaDTO.getTargetPassword());
            properties.put("driver",kafkaDTO.getTargetDriver());

            Dataset<Row> dataset = spark.read().jdbc(kafkaDTO.getTargetUrl(), tableName, properties);

            dataset.createOrReplaceGlobalTempView("OnlyOnceValidate_" + tableName);

        }

        Dataset<Row> result = spark.sql("select " + fieldName + " from " + "global_temp.OnlyOnceValidate_" + tableName  + " group by " + fieldName + " having count(" + fieldName + ") > 1");
        result.show();

        result.createOrReplaceGlobalTempView("OnlyOnceTemp_" + fieldName.toUpperCase());
    }
}
