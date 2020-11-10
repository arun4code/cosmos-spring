package com.pdata.batch.dto;

import com.microsoft.spring.data.gremlin.annotation.Vertex;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Vertex(label = "people2")
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class PeopleDTO {

	private String id;
    private Long peopleId;
    private String firstName;
    private String lastName;
    private String email;
    private int age;

}
