package com.pdata.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
//@ComponentScan(basePackages = { "com.pdata.batch.repository" })
@EnableBatchProcessing
@EnableSwagger2
public class PdataToGraphApplication {

	public static void main(String[] args) {
		SpringApplication.run(PdataToGraphApplication.class, args);
	}

}
