package com.gamll.publishrealtime.bean;

import com.sun.corba.se.spi.ior.ObjectKey;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NameValue {
    private  String name;
    private Object  value;
}
