package com.audit.hive;

import com.audit.bean.KafkaDTO;
import com.audit.bean.TaskProperty;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.List;

public class HiveDDL {

    public static void CreateHiveDDL(KafkaDTO kafkaDTO) {

        SparkSession ssc = SparkSession.builder().getOrCreate();

        StringBuffer hiveddl = new StringBuffer();
        hiveddl.append("Create external table if not exists " + kafkaDTO.getDatabaseName() + "." + kafkaDTO.getTableName() + "(");

        List<TaskProperty> rules = kafkaDTO.getRules();
        Iterator<TaskProperty> iterator = rules.iterator();

        while (iterator.hasNext()) {
            hiveddl.append(iterator.next().getFieldName() + " " + iterator.next().getFieldType());
            if (iterator.hasNext()) {
                hiveddl.append(", ");
            }
        }

        hiveddl.append(");");

        ssc.sql(hiveddl.toString());
    }

}
