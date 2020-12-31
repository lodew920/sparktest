package com;



import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

public class ddd {

    public static void main(String[] args) throws ClassNotFoundException {
        Class c = Class.forName("java.lang.String");
        System.out.println(c.toGenericString());
        System.out.println(c.toString());
        System.out.println("hello gxuejian ");
//        Stream.Builder<Object> builder = Stream.builder();
//        int length = Collection.class.getFields().length;
//        Arrays.stream();
//        boolean equals = new Collection<>().stream().collect().getClass().getName().equals();
//        () -> () -> () -> Collection.class.getName().equals();


    }
}