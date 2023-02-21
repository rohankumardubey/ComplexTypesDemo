package com.learning.spark.examples;

import javafx.scene.input.DataFormat;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
//10.9.2.81
public class sampleJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[3]")
                .appName("Complex Types Demo")

                .getOrCreate();


//        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");
//        spark.sql("set spark.sql.legacy.charVarcharAsString=true");

        Dataset<Row> parqDF = spark.read().parquet("C:\\GitProject\\ComplexTypesDemo\\src\\main\\scala\\" +
                "com\\learning\\spark\\examples\\" +
                "part-00000-deb93f20-b2d5-46b1-9183-8487b42ebfa5-c000.snappy.parquet");


//        System.out.println(parqDF.schema());
//        parqDF.show(false);
//        Dataset<Row> parqDF = spark.read().parquet("C:\\Users\\rdubey\\OneDrive - MODEL N, INC\\Desktop\\part-00000-4f3ea63c-abfa-4216-a441-fcc0c5b4bec6-c000.snappy.parquet");
////        parqDF.printSchema();
//        StructType struct = parqDF.schema();
//        parqDF.limit(100).show(false);

//        parqDF.limit(5).show(false);
//        System.out.println(struct.sql());
//        System.out.println(struct.json());
//        System.out.println(struct.prettyJson());

//        StructType Id = parqDF.select("Id").schema();
//        StructType Name = parqDF.select("Name").schema();
//        StructType HST1806__Parent__c = parqDF.select("HST1806__Parent__c").schema();
//        StructType HST1806__Type__c = parqDF.select("HST1806__Type__c").schema();
//        StructType HST1806__SubType__c = parqDF.select("HST1806__SubType__c").schema();
//        StructType HST1806__IsKit__c = parqDF.select("HST1806__IsKit__c").schema();
//        StructType HST1806__Parent__c_ancestors = parqDF.select("HST1806__Parent__c_ancestors").schema();
//        StructType children = parqDF.select("children").schema();

//        parqDF.select(functions.col("Id"),functions.col("Name"),functions.to_json(functions.col("ParentId")),functions.col("Type"),functions.to_json(functions.col("ParentId_ancestors")),functions.to_json(functions.col("children"))).write().option("header",true).option("sep","|").csv("C:\\Users\\rdubey\\OneDrive - MODEL N, INC\\Desktop\\part-00000-4f3ea63c-abfa-4216-a441-fcc0c5b4bec6-c000.csv");


//        List<StructField> structfields = new ArrayList<>();
//        structfields.add(new StructField("one",DataTypes.StringType,true,null));
//        structfields.add(new StructField("two",DataTypes.StringType,true,null));
//
//        List<StructField> structfields2 = new ArrayList<>();
//        structfields2.add(new StructField("one",DataTypes.IntegerType,true,null));
//        structfields2.add(new StructField("two",DataTypes.IntegerType,true,null));
//        StructType details = new StructType(new StructField[]{
//                new StructField("arraystring", DataTypes.createArrayType(DataTypes.StringType), false, null),
//                new StructField("arrayint",DataTypes.createArrayType(DataTypes.IntegerType), false, null),
//                new StructField("singlestruct",DataTypes.createStructType(structfields),false,null),
//                new StructField("arrayofStruct",DataTypes.createArrayType(DataTypes.createStructType(structfields2)),false,null),
//                new StructField("arrayofDecimal",DataTypes.createArrayType(DataTypes.createDecimalType(10,1)),false,null)
//
//        });
//|Id                |Name                           |ParentId                                                                                                                                                                                              |Type|HST1806__MnCustomerType__c|ParentId_ancestors    |children


//        Dataset<Row> parqDF = spark.read().parquet("C:\\Users\\rdubey\\OneDrive - MODEL N, INC\\Desktop\\part-00000-4f3ea63c-abfa-4216-a441-fcc0c5b4bec6-c000.snappy.parquet");
//        StructType struct = parqDF.schema();
//        struct.add(new StructField("decimal",DataTypes.createArrayType(DataTypes.createDecimalType(2,2)),false,null));
//        Dataset<Row> modifiedDataSet = spark.read().format("csv").option("header", false).option("sep", "|").load("C:\\Users\\rdubey\\OneDrive - MODEL N, INC\\Desktop\\date_time.csv");
//        Dataset<Row> mod = null;
//        System.out.println(Arrays.toString(modifiedDataSet.schema().fields()));

//        List<String> sample = new ArrayList<>();
//        sample.add("Hello");
//        sample.add("World");
//        System.out.println(sample.stream().anyMatch("hello"::equalsIgnoreCase));
//        modifiedDataSet.limit(10).show(false);
//        modifiedDataSet.printSchema();

        ////        Dataset<Row> modifiedDataSet = spark.read().format("csv").option("header", true).option("sep", "|").load("C:\\GitProject\\SparkProgrammingInScala\\22-ComplexTypesDemo\\src\\main\\scala\\guru\\learningjournal\\spark\\examples\\complex_data.csv");


//        int count = 0;
//        String sample =null;
//        for (StructField field : details.fields()) {
////            System.out.println("---------------------------");
////            System.out.println(field.dataType().sql());
////            System.out.println(field.dataType().json());
////            System.out.println(field.dataType().jsonValue());
////            System.out.println(field.dataType().prettyJson());
////            System.out.println(field.dataType().catalogString());
////            sample =field.dataType().catalogString();
////            System.out.println("---------------------------");
////            System.out.println("field.dataType().typeName():"+field.dataType().typeName());
////            System.out.println("field.dataType().catalogString(): "+field.dataType().catalogString());
////            System.out.println("field.dataType().sql():"+field.dataType().sql());
////            System.out.println("field.dataType().defaultConcreteType():"+field.dataType().defaultConcreteType());
////            System.out.println("field.dataType().getClass().getComponentType():"+field.dataType().getClass().getComponentType());
////            System.out.println("field.dataType().getClass().getCanonicalName():"+field.dataType().getClass().getCanonicalName());
////            System.out.println("field.dataType().getClass().getTypeName():"+field.dataType().getClass().getTypeName());
////            System.out.println("field.dataType().getClass().getTypeName():"+field.dataType().sameType(DataTypes.IntegerType));
////            System.out.println("field.dataType().getClass().getTypeName():"+field.dataType());
////            ComplexArrayType(field.name(),field.dataType());
////            modifiedDataSet = modifiedDataSet.withColumn(field.name(), functions.from_json(functions.col(field.name()), field.dataType()));
//
//        }
//        StructType schema = new StructType();

//        StructType structType = new StructType();
//        structType = structType.add("A", DataTypes.StringType, false);
//        structType = structType.add("B", DataTypes.createArrayType(DataTypes.StringType), false);
//        structType = structType.add("C", DataTypes.IntegerType, false);
//        structType = structType.add("D", DataTypes.FloatType, false);
//        List<Row> nums = new ArrayList<>();
//        List<String> numsample = new ArrayList<>();
//        numsample.add("adf");
//        numsample.add("def");
//        nums.add(RowFactory.create("value1", numsample,12,1.2));
//        nums.add(RowFactory.create("value2", numsample,13,1.3));
//        nums.add(RowFactory.create("value3", numsample,14,1.4));
//
//        Dataset<Row> df = spark.createDataFrame(nums, structType);

        parqDF=parqDF.withColumn("ParentId",functions.to_json(functions.col("ParentId")));
//        System.out.println(parqDF.schema());
        for (StructField field : parqDF.schema().fields()) {
            parqDF = parqDF.withColumn(field.name(), functions.col(field.name()).cast(DataTypes.StringType));
        }


        parqDF.schema();


//        for(StructField field:parqDF.schema().fields()){
//
//        }
        final Dataset<Row> sample=parqDF;

//        sample
        System.out.printf(String.valueOf(sample.storageLevel()));

        System.out.println(sample.toJavaRDD().getNumPartitions());
        final Dataset<Row> reparationData =sample.coalesce(10);
        final Dataset<Row> coalesceData =sample.coalesce(10);
        System.out.println(reparationData.toJavaRDD().getNumPartitions());
        System.out.println(coalesceData.toJavaRDD().getNumPartitions());
        parqDF.write().format("csv").option("delimiter","|").option("header",true).save("C:\\GitProject\\ComplexTypesDemo\\src\\main\\scala\\com\\learning\\spark\\examples\\Dump4");
//        df=df.withColumn("B",  functions.flatten(functions.col("B")));
//        data.withColumn(column, functions.from_json(functions.col(column), dataType));
//        StructType schema = new StructType();
//        Map<String,String> colsMap = new HashMap<>();
//        colsMap.put("time","timestamp");
//        colsMap.put("date","date");
//        colsMap.put("account","integer");
//        colsMap.put("rate","decimal(10,2)");
//        Iterator<Map.Entry<String, String>> itr = colsMap.entrySet().iterator();
//        while (itr.hasNext()) {
//            Map.Entry<String, String> entry = itr.next();
//            schema = (StructType) schema.add(entry.getKey(), entry.getValue());
//        }
//        Dataset<Row> data=spark.createDataFrame(new ArrayList<Row>(), schema);
//        System.out.println(data.schema());

//        mod = modifiedDataSet.select("Timestamp").withColumn("Timestamp",modifiedDataSet.select("Timestamp").col("Timestamp"));
//        mod = modifiedDataSet.select("date").withColumn("date",modifiedDataSet.select("date").col("date"));
//
//        mod.show(false);
//        mod =modifiedDataSet.toDF("sample","dump");
//        mod.show(false);

//        String sample =DataTypes.createStructType(new StructField[]{}).jsonValue().values().get("type").get().toString();
//        System.out.println(sample);
//        System.out.println(DataTypes.DateType.catalogString());
//        System.out.println(DataTypes.TimestampType.catalogString());
//        System.out.println(DataTypes.createArrayType(DataTypes.StringType).jsonValue().values().get("type").get());

//        System.out.println(StringUtils.trim("2018-12-11 HH:mm:ss"));
//        System.out.println(details.catalogString());
//        System.out.println(details.defaultSize());
//        System.out.println(details.distinct());
//        System.out.println(details.head());
//        System.out.println(details.fields()[details.fields().length-1]);
//        System.out.println(Arrays.toString(details.fields()));

//        System.out.println(modifiedDataSet.columns());
//
//        modifiedDataSet.columns()
//        Dataset<Row>  modifiedDataSet1 = modifiedDataSet.withColumn("timestamp", functions.to_timestamp(functions.col("timestamp")));
//        Dataset<Row> modifiedDataSet2 = modifiedDataSet1.withColumn("date", functions.to_date(functions.col("date")));
//
//        modifiedDataSet2.select("timestamp","date").show(false);
//        modifiedDataSet2.select("timestamp","date").printSchema();

//        HashMap<String,String> sam =gettingDecimalPoints("array<decimal(11121,1112)>");
//        System.out.println(sam);
//
//        String s = "array<decimal(121,12)>";


//        String answer = s.substring(s.indexOf("(")+1, s.indexOf(")"));
//        System.out.println(answer);
//        modifiedDataSet = modifiedDataSet.withColumn("HST1806__Parent__c", functions.from_json(functions.col("HST1806__Parent__c"), HST1806__Parent__c));
//        for (StructField field : struct.fields()) {
////        for (StructField field : details.fields()) {
//            if (field.dataType().typeName().equals("struct")) {
//                modifiedDataSet = modifiedDataSet.withColumn(field.name(), functions.from_json(functions.col(field.name()), field.dataType()));
//            } else if (field.dataType().typeName().equals("array")) {
//                modifiedDataSet = ComplexArrayDataType(field.name(), modifiedDataSet, field.dataType());
//            } else if (field.dataType().typeName().equals("date")) {
//                modifiedDataSet = ComplexDateType(field.name(), modifiedDataSet);
//            } else if (field.dataType().typeName().equals("timestamp")) {
//                modifiedDataSet = ComplexTimeStamp(field.name(), modifiedDataSet);
//            }
//        }

//        modifiedDataSet.limit(20).show(false);
//        modifiedDataSet.printSchema();
//
//        Set<String> dsFields = new HashSet<String>(asList(modifiedDataSet.schema().fieldNames()));
//        System.out.println(dsFields);
//        System.out.println("--------------");
//        dsFields = dsFields.stream().map(String::toLowerCase).collect(Collectors.toSet());
//        System.out.println(dsFields);
//        System.out.println("--------------");
//
//        LinkedHashMap<String, StructField> entityFieldMap = new LinkedHashMap<String, StructField>();
//            for (StructField field : modifiedDataSet.schema().fields()) {
//                entityFieldMap.put(field.name().toLowerCase(), field);
//        }
//        System.out.println(entityFieldMap);
//            entityFieldMap.remove("timestamp");
//
//        if (dsFields.containsAll(entityFieldMap.keySet())) {
//            System.out.println("what");
//        }
//        modifiedDataSet = ComplexDateType("date", modifiedDataSet);
//        modifiedDataSet = modifiedDataSet.withColumn("date", functions.to_date(functions.col("date"), "yyyy-M-d"));
//        modifiedDataSet = modifiedDataSet.withColumn("timestamp", functions.to_timestamp(functions.col("timestamp"), "M-d-yyyy HH:mm:SS"));
//        modifiedDataSet.select("date","timestamp").limit(10).show(false);
//        modifiedDataSet.select(functions.col("Id"),functions.col("Name"),functions.to_json(functions.col("ParentId")),functions.col("Type"),functions.to_json(functions.col("ParentId_ancestors")),functions.to_json(functions.col("children")),functions.to_json(functions.col("date")),functions.to_json(functions.col("timestamp")));

//        modifiedDataSet.write().option("header",true).parquet("C:\\Users\\rdubey\\OneDrive - MODEL N, INC\\Desktop\\complex_type_sample.parquet");
//
//
//        Dataset<Row> df =  spark.read().format("parquet").option("header", true).load("C:\\Users\\rdubey\\OneDrive - MODEL N, INC\\Desktop\\dump.parquet");
//
//        df.printSchema();
//        df.limit(10).show(false);
//        System.out.println(struct);
//        System.out.println(details);
//        while(true){
//            int random = new Random().nextInt(50);
//            modifiedDataSet.limit(random).show(false);
//            long dump =modifiedDataSet.count();
//            modifiedDataSet.printSchema();
//            Dataset<Row> sample =modifiedDataSet.unpersist();
//            sample.limit(100);
//            modifiedDataSet.show(false);
//            Dataset<Row> result = modifiedDataSet.crossJoin(sample);
//            Dataset<Row> result2 = result.crossJoin(sample);
//            result2.show(false);
//            result2.unpersist();
//            result.unpersist();
//            modifiedDataSet.unpersist();
//            Dataset<Row> sam = spark.read().format("csv").option("header", true).option("sep", "|").load("C:\\GitProject\\SparkProgrammingInScala\\22-ComplexTypesDemo\\src\\main\\scala\\guru\\learningjournal\\spark\\examples\\part-00000-ed09fbd7-5442-49cb-bf45-53749a2e7d54-c000-pipe.csv");
//            sam.unpersist();
//            sam.count();
//            sam.show(false);
//            Dataset<Row> dum =sam.crossJoin(result2);
//            Dataset<Row> res =dum.crossJoin(result2);
//            res.show(false);
//        }
    }

//    protected static Dataset<Row> ComplexArrayDataType(String column, Dataset<Row> data, DataType datatype) {
//        if (datatype.defaultConcreteType()==DataTypes.DateType) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.StringType)));
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.IntegerType))) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.IntegerType)));
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.DateType))) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.DateType)));
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.LongType))) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.LongType)));
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.FloatType))) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.FloatType)));
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.BooleanType))) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.BooleanType)));
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.DoubleType))) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.DoubleType)));
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.TimestampType))) {
//            data = data.withColumn(column, functions.from_json(functions.col(column), DataTypes.createArrayType(DataTypes.TimestampType)));
//        }
//
//        return data;
//    }
//
//    protected static void ComplexArrayType(String column,DataType datatype) {
//        if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.StringType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("string type");
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.IntegerType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("integer type");
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.DateType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("datetype type");
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.LongType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("long type type");
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.FloatType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("float type");
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.BooleanType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("boolean type");
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.DoubleType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("double type");
//        } else if (datatype.getClass().isInstance(DataTypes.createArrayType(DataTypes.TimestampType))) {
//
//            System.out.println("column name:"+column);
//            System.out.println("timestamp type");
//        }
//
//    }

//
//    protected static HashMap<String,String> gettingDecimalPoints(String schema) {
//        HashMap<String,String> precession_scale = new HashMap<>();
//        int first_pointer = 0;
//        int second_pointer = schema.length() - 1;
//        while (first_pointer < second_pointer) {
//            if (schema.charAt(first_pointer) != '(') {
//                first_pointer++;
//            }
//            else{
//                System.out.println("first_pointer:"+first_pointer);
//                break;
//            }
//        }
//        while (first_pointer < second_pointer) {
//            if (schema.charAt(second_pointer) != ')') {
//                second_pointer--;
//            }
//            else{
//                System.out.println("second_pointer:"+second_pointer);
//                break;
//            }
//
//        }
//
//
//
//        schema=schema.substring(first_pointer+1,second_pointer);
//        System.out.println(schema);
//        precession_scale.put("precession",schema.split(",")[0]);
//        precession_scale.put("scale",schema.split(",")[1]);
//        return precession_scale;


//        for (int k = 0; k < schema.length(); k++) {
//            if (schema.charAt(k) == '<') {
//                index = k;
//            }
//        }

//        schema = schema.substring(index + 1, schema.length() - counter).trim();
//        List<String> intermediateList = Arrays.asList(schema.split(","));
//        for (String field : intermediateList) {
//            DataType types = getTypes(field.split(":")[1]);
//            structField.add(DataTypes.createStructField(field.split(":")[0], types, true));
//        }
//
//        return DataTypes.createStructType(structField);
    }

//    protected static StructType generateStructTypeSchema(DataType datatype) {
//        return generateArrayTypeSchema(datatype);
//    }
//
//    protected static Dataset<Row> ComplexDateType(String column, Dataset<Row> data) {
//        Dataset<Row> sampling = data.select(column).filter(functions.col(column).notEqual("null")).limit(1);
//        String item = null;
//        if (sampling.count() != 0) {
//            Iterator<Row> iter = sampling.toLocalIterator();
//            item = (iter.next()).toString();
//            if (item.charAt(0) == '[' && item.charAt(item.length() - 1) == ']') {
//                item = item.substring(1, item.length() - 1);
//            }
//        }
//        StringBuilder patternVerify = new StringBuilder();
//        String dateFormat = null;
//        if (item != null) {
//            if (!item.contains("/")) {
//                String[] pattern = item.split("-");
//                for (int i = 0; i < pattern.length; i++) {
//                    patternVerify.append(pattern[i].length());
//                }
//                if (patternVerify.toString().equals("422")) {
//                    dateFormat = "yyyy-MM-dd";
//                } else if (patternVerify.toString().equals("224")) {
//                    dateFormat = "dd-MM-yyyy";
//                } else {
//                    dateFormat = "yyyy-MM-dd";
//                }
//                data = data.withColumn(column, functions.to_date(functions.col(column), dateFormat));
//            } else {
//                throw new IllegalArgumentException("Date type cannot contain / character it should be in yyyy-MM-dd format!");
//            }
//        } else {
//            data = data.withColumn(column, functions.to_date(functions.col(column), "yyyy-MM-dd"));
//        }
//        return data;
//    }

//    protected static Dataset<Row> ComplexTimeStamp(String column, Dataset<Row> data) {
//        Dataset<Row> sampling = data.select(column).filter(functions.col(column).notEqual("null")).limit(1);
//        String item = null;
//        if (sampling.count() != 0) {
//            Iterator<Row> iter = sampling.toLocalIterator();
//            item = (iter.next()).toString();
//            if (item.charAt(0) == '[' && item.charAt(item.length() - 1) == ']') {
//                item = item.substring(1, item.length() - 1);
//            }
//        }
//        StringBuilder patternVerify = new StringBuilder();
//        String timeStampFormat = null;
//        if (item != null) {
//            if (!item.contains("/")) {
//                String[] patternWithoutTime = item.split(" ");
//                String[] pattern = patternWithoutTime[0].split("-");
//                for (int i = 0; i < pattern.length; i++) {
//                    patternVerify.append(pattern[i].length());
//                }
//                if (patternVerify.toString().equals("422")) {
//                    timeStampFormat = "yyyy-MM-dd HH:mm:ss";
//                } else if (patternVerify.toString().equals("224")) {
//                    timeStampFormat = "dd-MM-yyyy HH:mm:ss";
//                } else {
//                    timeStampFormat = "yyyy-MM-dd HH:mm:ss";
//                }
//                data = data.withColumn(column, functions.to_timestamp(functions.col(column), timeStampFormat));
//            } else {
//                throw new IllegalArgumentException("Time stamp type cannot contain / character it the date it should be in yyyy-MM-dd HH:mm:ss format!");
//            }
//        } else {
//            data = data.withColumn(column, functions.to_date(functions.col(column), "yyyy-MM-dd HH:mm:ss"));
//        }
//        return data;
//    }
//}

