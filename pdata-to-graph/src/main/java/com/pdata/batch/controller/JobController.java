package com.pdata.batch.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.pdata.batch.job.JobRunner;
import com.pdata.batch.service.PeopleService;

@RestController
@RequestMapping("/run")
public class JobController {

	private JobRunner jobRunner;

	@Autowired
	public JobController(JobRunner jobRunner) {
		this.jobRunner = jobRunner;
	}

	@GetMapping(value = "/job")
	public String runJob() {
		jobRunner.runBatchJob();
		return String.format("Job for people data collection submitted successfully.");
	}


	@Autowired
	PeopleService service;

	@GetMapping("/saveTestData")
	public ResponseEntity<String> testData() {

		service.saveTestData();
		return ResponseEntity.ok().body("Records are saved");
	}

	
}