package com.audit.validation.rule;

import com.audit.bean.DataSourceProperty;
import com.audit.bean.KafkaDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OnlyOnceValidateRule {

    public static void duplicatedFields(KafkaDTO kafkaDTO, String fieldName) {
        SparkSession ssc = SparkSession.builder().getOrCreate();

        Dataset<Row> result = ssc.sql("select " + fieldName + " from " + kafkaDTO.getDatabaseName() + "." + kafkaDTO.getTableName() + " group by " + fieldName + " having count(" + fieldName + ") > 1");
/*        String[] columns = result.columns();
        List<String> duplicatedFieldsList = Stream.of(columns).collect(Collectors.toList());*/


        /*List<Row> rows = result.collectAsList();
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("values", DataTypes.StringType, false));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataset = ssc.createDataFrame(rows, structType);*/

        result.createOrReplaceGlobalTempView("OnlyOnceTemp_" + fieldName.toUpperCase());
        ssc.sql("select * from global_temp.OnlyOnceTemp_" + fieldName.toUpperCase());
        ssc.sql("desc global_temp.OnlyOnceTemp_" + fieldName.toUpperCase());
    }
}
