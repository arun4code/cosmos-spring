package com.pdata.batch.service;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;

import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

@Configuration
@Slf4j
public class BulkExecutor2 {
	@Value("${azure.db.host}")
	private String host;

	@Value("${gremlin.masterKey}")
	private String masterKey;
	
	@Bean
	public DocumentBulkExecutor build() throws Exception {
		ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setMaxPoolSize(1000);
        DocumentClient client = new DocumentClient(
        		host,
        		masterKey,
                connectionPolicy,
                ConsistencyLevel.Session);
        String databaseId="AccessData1";
        String collectionId="AccessData1";
        String databaseLink = String.format("/dbs/%s", databaseId);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);

        ResourceResponse<Database> databaseResponse = null;
        Database readDatabase = null;
        try {
            databaseResponse = client.readDatabase(databaseLink, null);
            readDatabase = databaseResponse.getResource();

            System.out.println("Database already exists...");

        } catch (DocumentClientException dce) {
            if (dce.getStatusCode() == 404) {
                System.out.println("Attempting to create database since non-existent...");

                Database databaseDefinition = new Database();
                databaseDefinition.setId(databaseId);


                    client.createDatabase(databaseDefinition, null);


                databaseResponse = client.readDatabase(databaseLink, null);
                readDatabase = databaseResponse.getResource();
            } else {
                throw dce;
            }
        }

        ResourceResponse<DocumentCollection> collectionResponse = null;
        DocumentCollection readCollection = null;

        try {
            collectionResponse = client.readCollection(collectionLink, null);
            readCollection = collectionResponse.getResource();

            System.out.println("Collection already exists...");
        } catch (DocumentClientException dce) {
            if (dce.getStatusCode() == 404) {
                System.out.println("Attempting to create collection since non-existent...");

                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.setId(collectionId);

                PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                Collection<String> paths = new ArrayList<String>();
                paths.add("/country");
                partitionKeyDefinition.setPaths(paths);
                collectionDefinition.setPartitionKey(partitionKeyDefinition);

                RequestOptions options = new RequestOptions();
                options.setOfferThroughput(400);

                // create a collection
                client.createCollection(databaseLink, collectionDefinition, options);

                collectionResponse = client.readCollection(collectionLink, null);
                readCollection = collectionResponse.getResource();
            } else {
                throw dce;
            }
        }

        System.out.println(readCollection.getId());
        System.out.println(readDatabase.getId());

       
        // Set client's retry options high for initialization
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(30);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(9);

       // Builder pattern
        DocumentBulkExecutor bulkExecutorBuilder = DocumentBulkExecutor.builder().from(
                client,
                databaseId,
                collectionId,
                readCollection.getPartitionKey(),
                400).build() ;// throughput you want to allocate for bulk import out of the container's total throughput

        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);
        
         // Instantiate DocumentBulkExecutor
			/*
			 * try { DocumentBulkExecutor bulkExecutor = bulkExecutorBuilder.build(); // Set
			 * retries to 0 to pass complete control to bulk executor
			 * client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0
			 * ); client.getConnectionPolicy().getRetryOptions().
			 * setMaxRetryAttemptsOnThrottledRequests(0); BulkImportResponse
			 * bulkImportResponse = bulkExecutor.importAll(list, false, false, null);
			 * System.out.println(bulkImportResponse.getNumberOfDocumentsImported()); }
			 * catch (Exception e) { e.printStackTrace(); }
			 */
		return bulkExecutorBuilder;

	}
}
