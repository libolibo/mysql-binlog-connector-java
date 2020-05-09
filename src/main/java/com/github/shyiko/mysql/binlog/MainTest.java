package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: mysql-binlog-connector-java
 * @description: 测试
 * @author: libo
 * @create: 2020-05-07 10:05
 **/
public class MainTest {

    public static void main(String[] args) throws Exception {


//        System.out.println(event);

//        System.out.println(Integer.toBinaryString(1 << 3));
//        System.out.println(Integer.toBinaryString(1 << (1 << 3)));
//        System.out.println(1 << 1000);
//        System.out.println(1 << 8);

//        try(com.github.shyiko.mysql.binlog.io.ByteArrayInputStream inputStream = new ByteArrayInputStream(new FileInputStream("/Users/libo/libo"))){
//
//            System.out.println(inputStream.readInteger(2));
//        }

        try(BinaryLogFileReader reader = new BinaryLogFileReader(new File("/Users/libo/software/mysql/mysql-5.7.26-macos10.14-x86_64/logs/mysql_bin.000040"))){

            Event event = reader.readEvent();
            reader.readEvent();
            reader.readEvent();

        }

//        String str = "abc";
//        char[] charArray = new char[]{'a', 'b', 'c'};
//        System.out.println(str.equals(new String(charArray)));
//
//        System.out.println(str.chars());

    }
}
