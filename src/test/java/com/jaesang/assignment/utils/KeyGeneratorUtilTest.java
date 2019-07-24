package com.jaesang.assignment.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class KeyGeneratorUtilTest {

    KeyGeneratorUtil keyGeneratorUtil;

    @Before
    public void setUp() throws Exception {
        keyGeneratorUtil = new KeyGeneratorUtil();
    }

    @Test
    public void numberGenerateKey() {
        String word = "1-point";
        assertEquals("number", keyGeneratorUtil.generateKey(word));
    }

    @Test
    public void lowwerEnglishGenerateKey() {
        String word = "abc";
        assertEquals("a", keyGeneratorUtil.generateKey(word));
    }

    @Test
    public void upperEnglishGenerateKey() {
        String word = "Abc12";
        assertEquals("a", keyGeneratorUtil.generateKey(word));
    }

}