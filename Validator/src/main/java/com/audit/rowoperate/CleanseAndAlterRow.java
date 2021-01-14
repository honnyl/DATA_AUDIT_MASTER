package com.audit.rowoperate;


import com.audit.bean.ValidateCondition;
import com.audit.standardization.rule.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.List;

import static com.audit.utils.ConstantUtil.*;


/**
 * @Description: spark的函数
 * @Author: weisc
 * @CreateDate: 2020/5/27 14:32
 * @Version: 1.0
 */
public class CleanseAndAlterRow implements Function<Row, Row> {

    private StructType fields;
    private String[] fieldNames;
    private String taskId;
    private List<Tuple2<String,List<ValidateCondition>>> standardListList;
    private List<Tuple2<String,String>> field;

    public CleanseAndAlterRow(List<Tuple2<String,List<ValidateCondition>>> standardListList, String taskId, StructType fields, List<Tuple2<String,String>> field) {
        this.fields = fields;
        this.fieldNames = fields.fieldNames();
        this.taskId = taskId;
        this.standardListList = standardListList;
        this.field = field;
    }

    /***\
     * 实现方法，数据每行传入，进行相应的加工处理
     * @param row
     * @return
     * @throws Exception
     */
    @Override
    public Row call(Row row) throws Exception {
        String invalidateError = "";
        Object[] newValues = new Object[fieldNames.length];

        for (Tuple2<String,List<ValidateCondition>> scl : standardListList) {


            for (ValidateCondition vc : scl._2) {

                //获取稽核名称：regex
                String validateName = vc.getName();

                for (int idx = 0; idx < fieldNames.length; idx++) {
                    Object fieldValue = (row.isNullAt(idx) ? null : row.get(idx));
                    newValues[idx] = fieldValue;

                    for (Tuple2<String,String> name : field) {

                        if (scl._1.equals(name._1)) {

                            if (fieldNames[idx].toUpperCase().equals(name._2.toUpperCase())) {

                                //获取值

                                if (vc != null) {
                                    Object value = execValidateRule(fieldValue, fieldNames[idx], vc, validateName);
                                    newValues[idx] = value;
                                } else {
                                    throw new NullPointerException("The ValidateCondition is null");
                                }
                            }
                        }
                    }
                }

                row = RowFactory.create(newValues);
            }
        }

        return row;

    }

    /***
     * 执行规则
     * @param val
     * @param standardList
     * @return
     */
    public Object execValidateRule(Object val, String filedName, ValidateCondition standardList, String typeName) {
        Object value = null;

        StandardRule standardRule = getRule(typeName);
        value = standardRule.validateRowValue(val, filedName, standardList).getValue();

        return value;
    }

    /***
     * 得到相对应的规则
     * @param typeName
     * @return
     */
    public StandardRule getRule(String typeName) {
        StandardRule standardRule = null;
        switch (typeName) {
            case BASE64DECODE:
                standardRule = new Base64Decode();
                break;
            case BASE64ENCODE:
                standardRule = new Base64Encode();
                break;
            case DATETIME:
                standardRule = new DateTimeStandardizer();
                break;
            case DEFAULTVALUE:
                standardRule = new DefaultValueStandardizer();
                break;
            case LOWERCASE:
                standardRule = new LowercaseStandardizer();
                break;
            case MASKCREDITCARD:
                standardRule = new MaskLeavingLastFourDigitStandardizer();
                break;
            case CONTROLCHARACTERS:
                standardRule = new RemoveControlCharsStandardizer();
                break;
            case REGEXREPLACEMENT:
                standardRule = new SimpleRegexReplacer();
                break;
            case STRIPNONNUMERIC:
                standardRule = new StripNonNumeric();
                break;
            case TRIM:
                standardRule = new TrimStandardizer();
                break;
            case UPPERCASE:
                standardRule = new UppercaseStandardizer();
                break;
        }

        return standardRule;
    }
}
