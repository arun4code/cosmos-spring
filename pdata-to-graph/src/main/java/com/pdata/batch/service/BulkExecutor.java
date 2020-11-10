package com.pdata.batch.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;

import lombok.extern.slf4j.Slf4j;

//@Configuration
@Slf4j
public class BulkExecutor {
	@Value("${azure.db.host}")
	private String host;

	@Value("${gremlin.masterKey}")
	private String masterKey;
	
	@Bean
	public DocumentBulkExecutor build() throws Exception {
		log.info("start- to build DocumentBulkExecutor");
		DocumentClient client = null;
		ConnectionPolicy connectionPolicy = new ConnectionPolicy();
		connectionPolicy.setMaxPoolSize(100);
		connectionPolicy.setEnableEndpointDiscovery(true);
		connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
		List<String> location = new ArrayList<>();
		location.add("West Europe");

		connectionPolicy.setPreferredLocations(location);
		client = new DocumentClient(host, masterKey, connectionPolicy,
				com.microsoft.azure.documentdb.ConsistencyLevel.Eventual);

		client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(100);
		client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(10);

		PartitionKeyDefinition pKey = new PartitionKeyDefinition();
		pKey.set("Key1", "Key1");

		com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor.Builder bulkExecutorBuilder 
		= DocumentBulkExecutor.builder().from(client, "AccessData", "AccessData", pKey, 400);

		DocumentBulkExecutor bulkExecutor = bulkExecutorBuilder.build();
		log.info("END-DocumentBulkExecutor - " + bulkExecutor.toString());
		return bulkExecutor;
	}
}
