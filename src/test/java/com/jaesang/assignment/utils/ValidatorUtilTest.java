package com.jaesang.assignment.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ValidatorUtilTest {

    ValidatorUtil validatorUtil ;
    @Before
    public void setUp() throws Exception {
        validatorUtil = new ValidatorUtil();
    }

    @Test
    public void isValidWord() {
        String word = "123";
        assertTrue(validatorUtil.isValidWord(word));
    }

    @Test
    public void isNotValidWord() {
        String word = "&&123";
        assertTrue(validatorUtil.isNotValidWord(word));
    }

    @Test
    public void isValidateArgument() {

        String[] args = {"input/words.txt","output","3"};
        assertTrue(validatorUtil.isValidateArgument(args));
    }

    @Test
    public void isNotValidateArgument() {
        String[] args = {"words.txt","output","1"};
        assertTrue(validatorUtil.isNotValidateArgument(args));
    }
}