package com.jfcbxp.redisson.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;


@ToString
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Student {

    private String name;
    private int age;
    private String city;
    private List<Integer> marks;
}
