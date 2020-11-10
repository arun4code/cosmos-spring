package com.pdata.batch.graph.model;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;

import com.microsoft.spring.data.gremlin.annotation.Vertex;
import com.pdata.batch.dto.PeopleDTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

//@Entity
@Vertex(label="people")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PeopleGraph {

    @Id
    private String id;

    private String firstName;

    private String lastName;

    private String age;
    
    private String email;

    private String partition = "d8608501-7d0c-449b-82a1-6bd39ca70831";
    
	public static PeopleGraph of(PeopleDTO peopleDTO) {
		PeopleGraph people = new PeopleGraph();
		people.setAge(String.valueOf((peopleDTO.getAge())));
		people.setEmail(peopleDTO.getEmail());
		people.setFirstName(peopleDTO.getFirstName());
		people.setLastName(peopleDTO.getLastName());
		people.setId(String.valueOf(peopleDTO.getPeopleId()));
		return people;
	}

	public static List<PeopleGraph> of(List<PeopleDTO> peopleDTO) {
		List<PeopleGraph> poepleList = new ArrayList<>();
		peopleDTO.stream().forEach(item -> {
			poepleList.add(PeopleGraph.of(item));
		});
		return poepleList;
	}
}

