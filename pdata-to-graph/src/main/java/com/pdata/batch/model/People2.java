package com.pdata.batch.model;

import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
//@Entity(name = "iam_people_data")
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class People2 {

    @Id
    //@GeneratedValue(strategy = GenerationType.AUTO)
    //@Column(name = "id")
    private Long id;
    
    //@Column(name = "first_name")
    private String firstName;
    
    //@Column(name = "last_name")
    private String lastName;
    
    //@Column(name = "UEMPLOYEE_ID")
    private String employeeId;
    
    //@Column(name = "SUPERVISOR_IAMID")
    private String supervisorIamId;

}
