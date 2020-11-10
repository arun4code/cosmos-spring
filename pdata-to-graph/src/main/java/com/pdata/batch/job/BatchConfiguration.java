package com.pdata.batch.job;

import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;
import com.pdata.batch.dto.PeopleDTO;
import com.pdata.batch.mapper.PeopleDBRowMapper;
import com.pdata.batch.model.People;
import com.pdata.batch.processor.PeopleProcessor;

import net.minidev.json.JSONObject;

//@Configuration
public class BatchConfiguration {

	/*
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	
	@Autowired
	private DataSource dataSource;

	@Autowired
	private PeopleProcessor peopleProcessor;

	@Bean
	@Qualifier(value = "peopleData")
	public Job asyncJob() throws Exception {
		return this.jobBuilderFactory.get("Asynchronous Processing JOB")
				.incrementer(new RunIdIncrementer())
				.start(asyncManagerStep()).build();
	}

	
	@Bean
	public Step asyncManagerStep() throws Exception {
		return stepBuilderFactory.get("Asynchronous Processing : Read -> Process -> Write ")
				.<People, Future<PeopleDTO>>chunk(1000).reader(employeeDBReader()).processor(asyncProcessor())
				.writer(asyncWriter())
				//.taskExecutor(taskExecutor())
				.build();
	}

	@Bean
	public ItemStreamReader<People> employeeDBReader() {
		JdbcCursorItemReader<People> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setSql("select * from people");// where age > 190 AND age < 200");
		reader.setRowMapper(new PeopleDBRowMapper());
		reader.setVerifyCursorPosition(false);
		return reader;
	}

	@Bean
	public AsyncItemProcessor<People, PeopleDTO> asyncProcessor() {
		AsyncItemProcessor<People, PeopleDTO> asyncItemProcessor = new AsyncItemProcessor<>();
		asyncItemProcessor.setDelegate(peopleProcessor);
		asyncItemProcessor.setTaskExecutor(taskExecutor());

		return asyncItemProcessor;
	}

	@Bean
	public AsyncItemWriter<PeopleDTO> asyncWriter() throws Exception {
		AsyncItemWriter<PeopleDTO> asyncItemWriter = new AsyncItemWriter<>();
		asyncItemWriter.setDelegate(writeInThreads());
		return asyncItemWriter;
	}

	@Bean
	ItemWriter<PeopleDTO> writeInThreads() {
		try {
			final DocumentBulkExecutor bulkExecutor = this.build();
			return items -> {
				TaskRunner.runTask(items, bulkExecutor);
			};
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;		
	}
	
	String host = "";

	String masterKey = "==";

	@Bean
	public DocumentBulkExecutor build() throws Exception {
		ConnectionPolicy connectionPolicy = new ConnectionPolicy();
		connectionPolicy.setMaxPoolSize(100);
		connectionPolicy.setEnableEndpointDiscovery(true);
		connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
		DocumentClient client = new DocumentClient(host, masterKey, connectionPolicy,
				com.microsoft.azure.documentdb.ConsistencyLevel.Eventual);

		client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(30);
		client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(10);

		PartitionKeyDefinition pKey = new PartitionKeyDefinition();
		pKey.set("Key1", "Key1");
		com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor.Builder bulkExecutorBuilder = DocumentBulkExecutor
				.builder().from(client, "AccessData", "AccessData", pKey, 15000);

		DocumentBulkExecutor bulkExecutor = bulkExecutorBuilder.build();
		return bulkExecutor;
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


	private JSONObject createJsonObject(PeopleDTO peopleDTO) {
		JSONObject jsonObj2 = new JSONObject();
		jsonObj2.put("id", String.valueOf(peopleDTO.getPeopleId()));
		jsonObj2.put("peopleId", peopleDTO.getPeopleId());
		jsonObj2.put("firstName", peopleDTO.getFirstName());
		jsonObj2.put("lastName", peopleDTO.getLastName());
		jsonObj2.put("age", peopleDTO.getAge());
		return jsonObj2;
	}

	
*/

}
