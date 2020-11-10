package com.pdata.batch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.pdata.batch.dto.PeopleDTO2;
import com.pdata.batch.model.People2;

@Component
public class PeopleProcessor2 implements ItemProcessor<People2, PeopleDTO2> {

    @Override
    public PeopleDTO2 process(People2 employee) throws Exception {
        PeopleDTO2 employeeDTO = new PeopleDTO2();
        employeeDTO.setId(String.valueOf(employee.getId()));
        employeeDTO.setFirstName(employee.getFirstName());
        employeeDTO.setLastName(employee.getLastName());
        employeeDTO.setEmployeeId(employee.getEmployeeId());
        employeeDTO.setSupervisorIamId(employee.getSupervisorIamId());
        employeeDTO.setCountry("UK");
        return employeeDTO;
    }
}