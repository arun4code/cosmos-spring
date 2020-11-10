package com.pdata.batch.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import com.pdata.batch.model.People2;


@Component
public class PeopleDBRowMapper2 implements RowMapper<People2> {

    @Override
    public People2 mapRow(ResultSet resultSet, int i) throws SQLException {
    	People2 employee = new People2();
        
        employee.setId(resultSet.getLong("id"));
        employee.setFirstName(resultSet.getString("first_name"));
        employee.setLastName(resultSet.getString("last_name"));
        employee.setEmployeeId(resultSet.getString("UEMPLOYEE_ID"));
        employee.setSupervisorIamId(resultSet.getString("SUPERVISOR_IAMID"));
        return employee;
    }
}