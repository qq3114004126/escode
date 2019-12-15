package com.desheng.bigdata.es.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BigdataProduct {
    private String name;
    private String author;
    private float version;
}