package com.pdata.batch.job;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.pdata.batch.dto.PeopleDTO2;
import com.pdata.batch.job.listner.ReadStepListner;
import com.pdata.batch.job.listner.WriteStepListner;
import com.pdata.batch.mapper.PeopleDBRowMapper2;
import com.pdata.batch.model.People2;
import com.pdata.batch.processor.PeopleProcessor2;
import com.pdata.batch.service.PeopleGraphService;

//@Configuration
public class JobConfiguration2 {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Value("${azure.db.chunk.size}")
	private int chunkSize;
	
	@Autowired
	private PeopleProcessor2 peopleProcessor;
	
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
		return this.stepBuilderFactory.get("pdata-to-graphdb").<People2, PeopleDTO2>chunk(chunkSize)
				.reader(employeeDBReader())
				.processor(peopleProcessor)
				.writer(this.writeToGraph())
				.listener(writeStepListner())
				.listener(readStepListner())
				//.taskExecutor(taskExecutor())
				.build();
	}
	
	
	private ItemWriter<? super PeopleDTO2> writeToGraph() {
		return items-> {
			//service.bulkUploadToGraph2(items);
			service.bulkImportUsingStoredProcedure(items);
		};
	}

	@Bean
	public ItemStreamReader<People2> employeeDBReader() {
		JdbcCursorItemReader<People2> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setSql("select * from iam_people_data");// where people_id < 136123");// where age > 190 AND age < 200");
		
		//reader.setRowMapper(new PeopleDBRowMapper());
		
		reader.setRowMapper(new PeopleDBRowMapper2());
		
		return reader;
	}
	
	@Bean
	public TaskExecutor taskExecutor(){
	    SimpleAsyncTaskExecutor asyncTaskExecutor=new SimpleAsyncTaskExecutor("spring_batch");
	    asyncTaskExecutor.setConcurrencyLimit(5);
	    return asyncTaskExecutor;
	}
	
	/*
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
	*/
	
	@Bean WriteStepListner writeStepListner() {
		return new WriteStepListner();
	}
	
	@Bean 
	ReadStepListner readStepListner() {
		return new ReadStepListner();
	}
}
