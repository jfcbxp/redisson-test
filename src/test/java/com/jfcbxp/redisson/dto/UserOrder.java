package com.jfcbxp.redisson.dto;

import com.jfcbxp.redisson.enums.Category;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class UserOrder {
    private int id;
    private Category category;

}
