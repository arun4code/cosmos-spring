package com.pdata.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
//@Entity(name = "PEOPLE")
@AllArgsConstructor
@NoArgsConstructor
public class People {
    
    private Long peopleId;
    
    private String firstName;
    
    private String lastName;
    
    private int age;
    
    private String email;

	public static People of(String firstName, String lastName, int age, String email) {
		People p = new People();
		p.setFirstName(firstName);
		p.setLastName(lastName);
		p.setAge(age);
		p.setEmail(email);
		return p;
	}
}
