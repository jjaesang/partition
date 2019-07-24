package com.jaesang.assignment.utils;

/**
 * 단어에 대한 Key값 설정
 * Key는 파일명과 Partitioning할 때 사용
 */
public class KeyGeneratorUtil {

    private static final String NUMBER_KEY = "number";

    /**
     * 단어를 소문자화하여, 단어의 첫번째 글짜를 Key값으로 설정
     * 단어가 숫자로 시작할 경우, number로 변경하여 Key값 설정
     * @param word A12b
     * @return a
     */
    public String generateKey(String word) {

        String partitionKey;

        int prefixKey = (int) word.toLowerCase().charAt(0);
        if (isNumber(prefixKey))
            partitionKey = NUMBER_KEY;
        else
            partitionKey = Character.toString((char) prefixKey);

        return partitionKey;

    }

    /**
     * 숫자로 시작하는 단어인지 판단
     * @param prefixKey 1ab
     * @return true
     */
    private boolean isNumber(int prefixKey) {
        return prefixKey >= '0' && prefixKey <= '9';
    }

}
