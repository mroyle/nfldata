package com.doit.domain;

import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

public class DomainTests extends Assert {

    @Test
    public void allDomainClassesShouldBeSerializable(){
        Reflections reflections = new Reflections("com.doit.domain", new SubTypesScanner(false));
        Set<Class<? extends Object>> allClasses = reflections.getSubTypesOf(Object.class);
        for (Class clazz : allClasses){
            assertTrue(clazz.getCanonicalName() + " is not serializable", Arrays.asList(clazz.getInterfaces()).contains(Serializable.class));
        }
    }
}
