package com.example;


import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.apache.spark.sql.functions.col;
/**
 * Created by achat1 on 9/23/15.
 * Just an example to see if it works.
 */
@Component
public class CsvServices {
    @Autowired
    private SparkSession sparkSession;
    private SQLContext sqlContext;

    
    public String[] columns(String filename) {
        Dataset<Row> csv = sparkSession.read().format("csv").option("header","true").option("delimiter", ";")  .load(filename);
        csv.createOrReplaceTempView(filename.replace(".csv", ""));


        System.out.println(csv.columns()[1]);
        
         return csv.columns();
 
    }
    public Dataset<Row> Select(String filename,String query) {
    	
        Dataset<Row> csv = sparkSession.read().format("csv").option("header","true").option("delimiter", ";")  .load(filename);
        csv.createOrReplaceTempView("hd");
		Dataset<Row> res = sparkSession.sql(query);
    	return res;
    }
}