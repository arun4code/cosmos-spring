package com.pdata.batch.job;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class JobRunner {

    private static final Logger logger = LoggerFactory.getLogger(JobRunner.class);


    private JobLauncher simpleJobLauncher;
    private Job demo3;

    @Autowired
    public JobRunner(Job demo3, JobLauncher jobLauncher) {
        this.simpleJobLauncher = jobLauncher;
        this.demo3 = demo3;
    }


    @Async
    public void runBatchJob() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addDate("date", new Date(), true);
        runJob(demo3, jobParametersBuilder.toJobParameters());
    }


    public void runJob(Job job, JobParameters parameters) {
        try {
            JobExecution jobExecution = simpleJobLauncher.run(job, parameters);
        } catch (JobExecutionAlreadyRunningException e) {
            logger.info("Job with fileName={} is already running.");
        } catch (JobRestartException e) {
            logger.info("Job with fileName={} was not restarted.");
        } catch (JobInstanceAlreadyCompleteException e) {
            logger.info("Job with fileName={} already completed.");
        } catch (JobParametersInvalidException e) {
            logger.info("Invalid job parameters.");
        }
    }

}
