package com.pdata.batch.dto;

import java.io.Serializable;

import com.microsoft.spring.data.gremlin.annotation.Vertex;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Vertex(label = "people2")
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class PeopleDTO2 implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String id;
    private String employeeId;
    private String firstName;
    private String lastName;
    private String supervisorIamId;
    private String country = "UK";
}
