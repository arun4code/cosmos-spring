package com.pdata.batch.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.cosmos.CosmosClient;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;
import com.pdata.batch.dto.PeopleDTO;
import com.pdata.batch.dto.PeopleDTO2;
import com.pdata.batch.graph.model.Network;
import com.pdata.batch.graph.model.PeopleGraph2;
import com.pdata.batch.graph.model.Relation;
import com.pdata.batch.repository.NetworkRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PeopleGraphServiceImpl implements PeopleGraphService {

	@Autowired
	private DocumentBulkExecutor bulkExecutor;

	@Value("${azure.db.host}")
	private String host;

	// private String host = "https://iam-097-graphdb.documents.azure.com:443/";

	@Value("{gremlin.masterKey}")
	private String masterKey;

	private List<String> getJsonString(List<?> peopleDTOList) throws Exception {
		List<String> jsonList = new ArrayList<>();
		ObjectMapper objMapper = new ObjectMapper();

		peopleDTOList.stream().forEach(item -> {
			try {
				jsonList.add(objMapper.writeValueAsString(item));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});
		return jsonList;
	}

	@Override
	public void bulkUploadToGraph(List<? extends PeopleDTO> peopleDTOList) throws Exception {
		log.info("bulkUploadToGraph: uploading chunk with first id- " + Thread.currentThread() + " ---- "
				+ peopleDTOList.get(0).getId());

		List<String> documentList = getJsonString1(peopleDTOList);

		if (bulkExecutor == null) {
			log.error("error in build executor loader for azure db");
		}

		
		BulkImportResponse bulkImportResponse = bulkExecutor.importAll(documentList, true, true, 1000);

		// Validate that all documents inserted to ensure no failure.
		if (bulkImportResponse.getNumberOfDocumentsImported() < peopleDTOList.size()) {
			for (Exception e : bulkImportResponse.getErrors()) {
				// Validate why there were some failures.
				log.error("error to upload documents");
				e.printStackTrace();
			}
		}
		
		// Print statistics for this checkpoint				
		System.out.println(
				"##########################################################################################");

		// Print statistics for current checkpoint
		System.out.println("Number of documents inserted in this checkpoint: "
				+ bulkImportResponse.getNumberOfDocumentsImported());
		System.out.println("Import time for this checkpoint in milli seconds "
				+ bulkImportResponse.getTotalTimeTaken().toMillis());
		System.out.println("Total request unit consumed in this checkpoint: "
				+ bulkImportResponse.getTotalRequestUnitsConsumed());

		System.out.println("Average RUs/second in this checkpoint: "
				+ bulkImportResponse.getTotalRequestUnitsConsumed()
						/ (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
		System.out.println("Average #Inserts/second in this checkpoint: "
				+ bulkImportResponse.getNumberOfDocumentsImported()
						/ (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
		System.out.println(
				"##########################################################################################");

		

		log.info("Completed upload task for current thread with first id: " + Thread.currentThread() + " ---- "
				+ peopleDTOList.get(0).getId());
	}

	private List<String> getJsonString1(List<? extends PeopleDTO> peopleDTOList) {
		List<String> jsonList = new ArrayList<>();
		ObjectMapper objMapper = new ObjectMapper();

		peopleDTOList.stream().forEach(item -> {
			try {
				jsonList.add(objMapper.writeValueAsString(item));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});
		return jsonList;
	}

	// TODO: not in use
	@Override
	public void bulkUpload(List<PeopleDTO> peopleDTOList) throws Exception {
		System.out.println("bulkupload size -" + peopleDTOList.size());
		System.out.println("------ first id- " + Thread.currentThread() + " ---- " + peopleDTOList.get(0).getId());

		List<String> documentList = getJsonString(peopleDTOList);

		if (bulkExecutor == null) {
			System.out.println("--------");
		}
		// DocumentBulkExecutor bulkExecutor = build();
		BulkImportResponse bulkImportResponse = bulkExecutor.importAll(documentList, true, true, 100);

		// Validate that all documents inserted to ensure no failure.
		if (bulkImportResponse.getNumberOfDocumentsImported() < peopleDTOList.size()) {
			for (Exception e : bulkImportResponse.getErrors()) {
				// Validate why there were some failures.
				e.printStackTrace();
			}
		}

		// bulkExecutor.close();
		// client.close();

		System.out.println(bulkImportResponse);

	}

	@Autowired
	private NetworkRepository networkRepo;
	
	@Override
	public void bulkUploadToGraph2(List<? extends PeopleDTO2> peopleDTOList) throws Exception {
		List<Network> networkList = new ArrayList<>();
		Network network = new Network();
		//network.setId("N001");
		
		Map<String, PeopleDTO2> map = new HashMap<>();
		
		peopleDTOList.forEach(item -> {
			map.put(item.getEmployeeId(), item);
		});
		
		peopleDTOList.forEach(item -> {
			network.getVertexes().add(item);
			if (item.getSupervisorIamId() != null) {
				Relation relation = new Relation();
				//relation.setId("R001");
				relation.setName("manage");
				relation.setPersonFrom(PeopleGraph2.of(map.get(item.getSupervisorIamId())));
				relation.setPersonTo(PeopleGraph2.of(item));

				network.getEdges().add(relation);
			}
		});

		networkList.add(network);

		// this.networkRepo.save(this.network);
		List<String> documentList = getJsonString(networkList);

		if (bulkExecutor == null) {
			System.out.println("--------");
		}
		
		networkRepo.save(network);
		// DocumentBulkExecutor bulkExecutor = build();
		/*
		 * BulkImportResponse bulkImportResponse = bulkExecutor.importAll(documentList,
		 * true, false, 100);
		 * 
		 * // Validate that all documents inserted to ensure no failure. if
		 * (bulkImportResponse.getNumberOfDocumentsImported() < peopleDTOList.size()) {
		 * for (Exception e : bulkImportResponse.getErrors()) { // Validate why there
		 * were some failures. e.printStackTrace(); } }
		 * 
		 * System.out.println("Bad request size : " +
		 * bulkImportResponse.getBadInputDocuments().size());
		 */
	}
	
	
	@Override
	public void bulkImportUsingStoredProcedure(List<? extends PeopleDTO2> peopleDTOList) throws Exception {
		String databaseId="AccessData1";
        String collectionId="AccessData1";
        //String databaseLink = String.format("/dbs/%s", databaseId);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);
        
		/*
		StoredProcedure newStoredProcedure = new StoredProcedure(
		    "{" +
		        "  'id':'createPDataItemInBulk'," +
		        "  'body':" + new String(Files.readAllBytes(Paths.get("C:\\jar\\createPDataItemInBulk.js"))) +
		    "}");
	*/
        
		ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setMaxPoolSize(1000);
		DocumentClient client = new DocumentClient(
        		"host",
        		"pwd",
                connectionPolicy,
                ConsistencyLevel.Session);
        
        
        //CosmosClient cli = new CosmosClient(null);
        
        String sprocLink = String.format("%s/sprocs/%s", collectionLink, "createPDataItemInBulk");
        final CountDownLatch successfulCompletionLatch = new CountDownLatch(1);
        
        RequestOptions requestOptions = new RequestOptions();
		//PartitionKeyDefinition pKey = new PartitionKeyDefinition();
		//pKey.set("Key1", "Key1");

        requestOptions.setPartitionKey(new PartitionKey("UK"));
        
        List<PeopleDTO2> list = new ArrayList<>();
        peopleDTOList.stream().forEach(item -> {
        	list.add(item);
        });
        
        
Map<String, PeopleDTO2> map = new HashMap<>();
		
		peopleDTOList.forEach(item -> {
			map.put(item.getEmployeeId(), item);
		});
		
		List<Network> networkList = new ArrayList<>();
		Network network = new Network();
		peopleDTOList.forEach(item -> {
			network.getVertexes().add(item);
			if (item.getSupervisorIamId() != null) {
				Relation relation = new Relation();
				//relation.setId("R001");
				relation.setName("manage");
				relation.setPersonFrom(PeopleGraph2.of(map.get(item.getSupervisorIamId())));
				relation.setPersonTo(PeopleGraph2.of(item));

				network.getEdges().add(relation);
			}
		});

		networkList.add(network);
		
		
        Object[] storedProcedureArgs = new Object[] { list };
        client.executeStoredProcedure(sprocLink, requestOptions, storedProcedureArgs);
        
        
		/*
		 * .subscribe(storedProcedureResponse -> { String storedProcResultAsString =
		 * storedProcedureResponse.getResponseAsString();
		 * successfulCompletionLatch.countDown();
		 * System.out.println(storedProcedureResponse.getActivityId()); }, error -> {
		 * successfulCompletionLatch.countDown(); System.err.
		 * println("an error occurred while executing the stored procedure: actual cause: "
		 * + error.getMessage()); });
		 */
        
        
        System.out.println("----------");
        successfulCompletionLatch.await();
        
        
		//StoredProcedure createdStoredProcedure = client.createStoredProcedure(containerLink, newStoredProcedure, null)
		//    .toBlocking().single().getResource();
        
        
        
	}
	
	public static int getOfferThroughput(DocumentClient client, DocumentCollection collection) {
		FeedResponse<Offer> offers = client.queryOffers(
				String.format("SELECT * FROM c where c.offerResourceId = '%s'", collection.getResourceId()), null);

		List<Offer> offerAsList = offers.getQueryIterable().toList();
		if (offerAsList.isEmpty()) {
			throw new IllegalStateException("Cannot find Collection's corresponding offer");
		}

		Offer offer = offerAsList.get(0);
		return offer.getContent().getInt("offerThroughput");
	}
}
