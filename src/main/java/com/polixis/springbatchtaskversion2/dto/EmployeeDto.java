package com.polixis.springbatchtaskversion2.dto;

import com.polixis.springbatchtaskversion2.model.Employee;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EmployeeDto extends Employee {

    private String firstName;

    private String lastName;

    private String date;
}
