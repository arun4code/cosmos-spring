package com.pdata.batch.job2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.pdata.batch.dto.PeopleDTO;
import com.pdata.batch.mapper.PeopleDBRowMapper;
import com.pdata.batch.model.People;
import com.pdata.batch.processor.PeopleProcessor;
import com.pdata.batch.service.PeopleGraphService;


//@Configuration
public class JobConfiguration 
{
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;
	
	@Autowired
	PeopleGraphService service;

	@Bean
	public ColumnRangePartitioner partitioner() 
	{
		ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
		columnRangePartitioner.setColumn("people_id");
		columnRangePartitioner.setDataSource(dataSource);
		columnRangePartitioner.setTable("people");
		return columnRangePartitioner;
	}

	@Bean
	@StepScope
	public JdbcPagingItemReader<People> pagingItemReader(
			@Value("#{stepExecutionContext['minValue']}") Long minValue,
			@Value("#{stepExecutionContext['maxValue']}") Long maxValue) 
	{
		System.out.println("reading " + minValue + " to " + maxValue);

		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("people_id", Order.ASCENDING);
		
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("first_name, last_Name, people_id, age, email");
		queryProvider.setFromClause("from people ");
		queryProvider.setWhereClause("where people_id >= " + minValue + " and people_id < " + maxValue);
		queryProvider.setSortKeys(sortKeys);
		
		JdbcPagingItemReader<People> reader = new JdbcPagingItemReader<>();
		reader.setDataSource(this.dataSource);
		reader.setFetchSize(5000);
		reader.setRowMapper(new PeopleDBRowMapper());
		reader.setQueryProvider(queryProvider);
		
		return reader;
	}
	
	@Bean
	@StepScope
	public ItemWriter<? super PeopleDTO> writeToGraph() {
		return items-> {
			service.bulkUploadToGraph(items);
		};
	}
	
	// Master
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.partitioner(slaveStep().getName(), partitioner())
				.step(slaveStep())
				.gridSize(4)
				.taskExecutor(new SimpleAsyncTaskExecutor())
				.build();
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
	
	@Autowired
	private PeopleProcessor peopleProcessor;
	
	// slave step
	@Bean
	public Step slaveStep() 
	{
		return stepBuilderFactory.get("slaveStep")
				.<People, PeopleDTO>chunk(5000)
				.reader(pagingItemReader(null, null))
				.processor(peopleProcessor)
				.writer(writeToGraph())
				.build();
	}
	
	@Bean
	public Job job() 
	{
		return jobBuilderFactory.get("job")
				.start(step1())
				.build();
	}
}