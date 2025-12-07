package org.example;

import java.io.Serializable;

public class Person implements Serializable {

    private int id;
    private String name;
    private int age;

    public Person() {}

    public Person(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    // getters + setters
}
