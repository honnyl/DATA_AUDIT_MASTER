package com.audit.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used on Constructor parameters of the class that reference the {@link PolicyProperty} annotated fields in the class
 *
 * @see PolicyProperty
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface PolicyPropertyRef {

    String name();
}