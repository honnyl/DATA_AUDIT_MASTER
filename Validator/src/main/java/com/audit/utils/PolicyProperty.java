package com.audit.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that is used to display input form elements
 * the angular directive 'policy-input-form' along with the angular service 'PolicyInputFormService' are used to render these elements
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface PolicyProperty {


    /**
     * internal name for the property
     * @return String
     */
    String name();

    /**
     * The label for the form item
     * @return String
     */
    String displayName() default "";

    /**
     * The value of the input.  The form will default to this value if it is supplied
     * @return String
     */
    String value() default "";

    /**
     * Input placeholder with text indicating what should be entered
     * @return String
     */
    String placeholder() default "";

    /**
     * Determines how the property should be rendered:
     * number, string, select, regex, date, chips, feedChips, currentFeed, currentFeedCronSchedule, feedSelect, email, cron
     * type of {@link PolicyPropertyTypes.PROPERTY_TYPE#string} will be rendered as a standard input box.
     * You can enforce specific text expressions using the property passing in the valid Regexp
     * @return PolicyPropertyTypes.PROPERTY_TYPE
     */
    PolicyPropertyTypes.PROPERTY_TYPE type() default PolicyPropertyTypes.PROPERTY_TYPE.string;

    /**
     * Helpful text that will be rendered below the input entry
     * @return String
     */
    String hint() default "";

    /**
     * For Select inputs this is a list of the options that will be available.  This cannot be used with the {@code selectableValues()}
     * @return PropertyLabelValue[]
     */
    PropertyLabelValue[] labelValues() default {};


    /**
     * For a select input this is the list of optoins that will be available.  This cannot be used with the {@code labelValues()}
     * @return PropertyLabelValue[]
     */
    String[] selectableValues() default {};

    /**
     * Used for rendering {@link PolicyPropertyTypes.PROPERTY_TYPE#chips} where the input has multiple values
     * @return PropertyLabelValue[]
     */
    PropertyLabelValue[] values() default {};

    /**
     * requires input from the user.
     * @return boolean
     */
    boolean required() default false;

    /**
     * Indicates the input will be hidden
     * @return boolean
     */
    boolean hidden() default false;

    /**
     * A arbitrary string used to group inputs together.
     * Items with the same group name will be displayed together in the same row
     * @return String
     */
    String group() default "";

    /**
     * Regexp used to evaluate the input.
     * This should not include the beginning and ending slashes
     * Example. To validate a Numeric input one should use: ^\d+$  instead of this: /^\d+$/
     * @return String
     */
    String pattern() default "";

    /**
     * The message to be displayed if the pattern is supplied and the user enters something invalid
     * @return String
     */
    String patternInvalidMessage() default "Invalid Input";

    PropertyLabelValue[] additionalProperties() default {};

}
