package com.audit.standardization.test;

import com.alibaba.fastjson.JSONObject;
import com.audit.standardization.rule.DateTimeStandardizer;
import com.audit.standardization.rule.StripNonNumeric;
import org.apache.avro.Schema;

import java.util.*;

public class Test {
    public static void main1(String[] args) {

        int nums[] = new int[]{2, 7, 11, 15};
        int target = 9;

        int[] ints = twoSum(nums, target);

        System.out.println(Arrays.toString(ints));

        StringBuffer ppp3 = new StringBuffer();
        ppp3.reverse();

        String a = "ppppoo";

    }

    public static int[] twoSum(int[] nums, int target) {
        for(int i = 0;i < nums.length - 1;i++) {
            for (int j = i + 1; j < nums.length; j++) {
                if (target == nums[i] + nums[j]) {
                    return new int[]{i,j};
                }
            }
        }

        return null;
    }

    public static void main(String[] args) {

        LinkedHashMap map = JSONObject.parseObject("{\"phone\":\"int\",\"id\":\"int\",\"name\":\"string\",\"age\":\"string\",\"date\":\"string\"}", LinkedHashMap.class);

        //Iterator iter = map.descendingKeySet().iterator();

        //Iterator iter = map.values().iterator();

        /*while(iter.hasNext())
        {
            String s = iter.next().toString();
            System.out.println(s);
        }*/

        /*while(iter.hasNext())
        {
            String s = iter.next().toString();
            System.out.println(s);
        }
*/
    }


}





