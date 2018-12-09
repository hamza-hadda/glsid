package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by Owner on 2017. 03. 29..
 */
@RestController
@Controller
public class ApiController {
    @Autowired
    CsvServices csvServices;
    SQLContext sqlContext;

   /* @RequestMapping("wordcount")
    public ResponseEntity<List<Count>> words() {
        return new ResponseEntity<>(wordCount.count(), HttpStatus.OK);
    }*/
	@RequestMapping(value="/save" , method=RequestMethod.GET)
    public ResponseEntity<String[]> words() {
        return new ResponseEntity<>(csvServices.columns("src/main/resources/Korea Policy File 100k.csv"), HttpStatus.OK);
    }
    
	@RequestMapping(value="/select" , method=RequestMethod.GET)
    public ResponseEntity<Dataset<Row>> select() {
		
        return new ResponseEntity<>(csvServices.Select("src/main/resources/Korea Policy File 100k.csv","Select * from hd"), HttpStatus.OK);
    }
}
