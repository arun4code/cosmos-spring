package com.pdata.batch.graph.model;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;

import com.microsoft.spring.data.gremlin.annotation.Vertex;
import com.pdata.batch.dto.PeopleDTO2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Vertex(label="peoplegraph")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PeopleGraph2 {

    @Id
    private String id;

    private String firstName;

    private String lastName;

    private String employeeID;
    
    private String supervisorIAMID;

    private String partition = "d8608501-7d0c-449b-82a1-6bd39ca70831";
    
	public static PeopleGraph2 of(PeopleDTO2 peopleDTO) {
		PeopleGraph2 people = new PeopleGraph2();
		people.setId(peopleDTO.getId());
		people.setEmployeeID(peopleDTO.getEmployeeId());
		
		people.setFirstName(peopleDTO.getFirstName());
		people.setLastName(peopleDTO.getLastName());
		people.setSupervisorIAMID(peopleDTO.getSupervisorIamId());
		return people;
	}

	public static List<PeopleGraph2> of(List<PeopleDTO2> peopleDTO) {
		List<PeopleGraph2> poepleList = new ArrayList<>();
		peopleDTO.stream().forEach(item -> {
			poepleList.add(PeopleGraph2.of(item));
		});
		return poepleList;
	}
}

