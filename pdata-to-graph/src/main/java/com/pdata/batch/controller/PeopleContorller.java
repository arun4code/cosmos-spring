package com.pdata.batch.controller;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pdata.batch.dto.PeopleDTO;
import com.pdata.batch.graph.model.PeopleGraph;
import com.pdata.batch.repository.PeropleGraphRepository;
import com.pdata.batch.service.PeopleGraphService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/v1/application")
@Slf4j
public class PeopleContorller {

	@Autowired
	PeropleGraphRepository repo;

	@Autowired
	PeopleGraphService service;

	@GetMapping("/consumeJsonMessage")
	public PeopleDTO consumeJsonMessage() {
		return people;
	}

	PeopleDTO people = null;

	public static List<PeopleDTO> fromJSON(final TypeReference<List<PeopleDTO>> type, final String jsonPacket) {
		List<PeopleDTO> data = null;

		try {
			data = new ObjectMapper().readValue(jsonPacket, type);
		} catch (Exception e) {
			// Handle the problem
		}
		return data;
	}

	@GetMapping(value = "/people/{peopleId}", produces = { MediaType.APPLICATION_JSON_VALUE })
	public ResponseEntity<PeopleGraph> getPeople(@PathVariable String peopleId) {

		Optional<PeopleGraph> people = repo.findById(peopleId);

		log.info("people data : {}", people.get());

		return new ResponseEntity<PeopleGraph>(people.get(), HttpStatus.OK);
	}

	@GetMapping(value = "/peopleCount", produces = { MediaType.APPLICATION_JSON_VALUE })
	public ResponseEntity<String> getPeopleCount() {

		long count = repo.count();
		;

		return new ResponseEntity<String>("Total Count : " + count, HttpStatus.OK);
	}

	@PostMapping(value = "/saveData", produces = { MediaType.APPLICATION_JSON_VALUE }, consumes = {
			MediaType.APPLICATION_JSON_VALUE })
	public ResponseEntity<String> createAccount(@RequestBody PeopleDTO peopleDTO) {
		PeopleGraph people = PeopleGraph.of(peopleDTO);
		repo.save(people);

		return new ResponseEntity<>("Created", HttpStatus.CREATED);
	}

	@PostMapping(value = "/savePeopleData", produces = { MediaType.APPLICATION_JSON_VALUE }, consumes = {
			MediaType.APPLICATION_JSON_VALUE })
	public ResponseEntity<String> createPeople(@RequestBody List<PeopleDTO> peopleDTOList) {
		try {
			service.bulkUpload(peopleDTOList);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new ResponseEntity<>("Created", HttpStatus.CREATED);
	}

	@DeleteMapping(value = "/deletePeopleData")
	public ResponseEntity<String> deletePeople() {
		repo.deleteAll();

		return new ResponseEntity<>("deleted", HttpStatus.ACCEPTED);
	}

}
