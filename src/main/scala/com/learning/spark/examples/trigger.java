package com.learning.spark.examples;

import java.util.HashMap;

public class trigger {
    public static void main(String[] args) {

        long initialval =System.nanoTime();
        System.out.println(sample("array<decimal(11121,1112)>"));
        long finalval=System.nanoTime();

        long initialget =System.nanoTime();
        System.out.println(gettingDecimalPoints("array<decimal(11121,1112)>"));
        long finalvalget=System.nanoTime();


        System.out.println("sample method "+(finalval-initialval));
        System.out.println("gettingDecimalPoints method "+(finalvalget-initialget));

//        System.out.println(initialval);
//        System.out.println(finalval);
    }


    protected static HashMap<String, Integer> sample(String schema){
        HashMap<String,Integer> precession_scale = new HashMap<>();
        schema= schema.substring(schema.indexOf('(')+1,schema.indexOf(')'));
        precession_scale.put("precision", Integer.valueOf(schema.split(",")[0]));
        precession_scale.put("scale", Integer.valueOf(schema.split(",")[1]));
        return precession_scale;
    }
    protected static HashMap<String,Integer> gettingDecimalPoints(String schema) {
        HashMap<String,Integer> precession_scale = new HashMap<>();
        int first_pointer = 0;
        int second_pointer = schema.length() - 1;
        while (first_pointer < second_pointer) {
            if (schema.charAt(first_pointer) != '(') {
                first_pointer++;
            } else {
                break;
            }
        }
        while (first_pointer < second_pointer) {
            if (schema.charAt(second_pointer) != ')') {
                second_pointer--;
            } else {
                break;
            }

        }

        schema=schema.substring(first_pointer+1,second_pointer);

        precession_scale.put("precision", Integer.valueOf(schema.split(",")[0]));
        precession_scale.put("scale", Integer.valueOf(schema.split(",")[1]));
        return precession_scale;
    }
}
