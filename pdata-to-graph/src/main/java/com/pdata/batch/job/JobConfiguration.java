package com.pdata.batch.job;

import java.util.concurrent.ThreadPoolExecutor;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.pdata.batch.dto.PeopleDTO;
import com.pdata.batch.job.listner.ReadStepListner;
import com.pdata.batch.job.listner.WriteStepListner;
import com.pdata.batch.mapper.PeopleDBRowMapper;
import com.pdata.batch.model.People;
import com.pdata.batch.processor.PeopleProcessor;
import com.pdata.batch.service.PeopleGraphService;

@Configuration
public class JobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Value("${azure.db.chunk.size}")
	private int chunkSize;
	
	@Autowired
	private PeopleProcessor peopleProcessor;
	
	@Autowired
	PeopleGraphService service;
	
	@Qualifier(value = "peopleData")
	@Bean
	public Job peopleJob() throws Exception {
		return this.jobBuilderFactory.get("peopleData")
				.incrementer(new RunIdIncrementer())
				.flow(steps()).end().build();
				//.start(steps()).build();
	}

	
	@Bean
	//@StepScope
	public Step steps() throws Exception {
		return this.stepBuilderFactory.get("spring_batch").<People, PeopleDTO>chunk(chunkSize)
				.reader(employeeDBReader())
				.processor(peopleProcessor)
				.writer(this.writeToGraph())
				.listener(writeStepListner())
				.listener(readStepListner())
				//.taskExecutor(taskExecutor())
				.build();
	}
	
	
	private ItemWriter<? super PeopleDTO> writeToGraph() {
		return items-> {
			service.bulkUploadToGraph(items);
		};
	}

	@Bean
	public ItemStreamReader<People> employeeDBReader() {
		JdbcCursorItemReader<People> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setSql("SELECT * FROM mydb.people");// where people_id < 108300");// where age > 190 AND age < 200");
		reader.setRowMapper(new PeopleDBRowMapper());
		return reader;
	}
	

	
	@Bean
	public TaskExecutor taskExecutor() {
	    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
	    executor.setCorePoolSize(64);
	    executor.setMaxPoolSize(64);
	    executor.setQueueCapacity(64);
	    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
	    executor.setThreadNamePrefix("MultiThreaded-");
	    return executor;
	}
	
	@Bean WriteStepListner writeStepListner() {
		return new WriteStepListner();
	}
	
	@Bean 
	ReadStepListner readStepListner() {
		return new ReadStepListner();
	}
}
