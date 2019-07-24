package com.jaesang.assignment.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.util.regex.Pattern;

/**
 * 해당 프로세스내에서 사용되는 Validation 함수들
 */
public class ValidatorUtil {

    private static Logger logger = Logger.getLogger(ValidatorUtil.class);

    private static final String VALID_PATTERN = "^[a-zA-Z0-9].*";

    /**
     * 들어온 단어가 유효한 단어인지 판단함
     * @param word !ab
     * @return false
     */
    public boolean isValidWord(String word) {
        return Pattern.matches(VALID_PATTERN, word);
    }

    public boolean isNotValidWord(String word) {
        return !isValidWord(word);
    }

    /**
     * Argument 3개에 대한 Validation을 수행함
     * @param args
     * @return
     */
    public boolean isValidateArgument(String[] args) {

        if (args.length != 3) {
            logger.error("argument size MUST BE 3, " + args.length + " is not valid ..");
            return false;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String numberOfPartition = args[2];

        if (isNotAccessableFile(inputPath)){
            logger.error("input File " + inputPath +" can not accessable ..");
            return false;
        }

        if (isNotAccessableDir(outputPath)){
            logger.error("output Path " + outputPath +" is not existed director or can not accessable ..");
            return false;
        }


        if (isNotValidPartitionNum(numberOfPartition)){
            logger.error("partitionNum MUST BE Integer And 1<N<28 ,"+ numberOfPartition + " is not valided");
            return false;
        }


        return true;

    }


    public boolean isNotValidateArgument(String[] args) {
        return !isValidateArgument(args);
    }

    /**
     * Input 파일이 File이면서 존재하는지 Validation
     * @param inputPath input/words.txt
     * @return true
     */
    private boolean isAccessableFile(String inputPath) {
        File inputFile = new File(inputPath);
        return inputFile.isFile() && !inputFile.isDirectory() && inputFile.exists();
    }

    private boolean isNotAccessableFile(String inputPath) {
        return !isAccessableFile(inputPath);
    }

    /**
     * Output 디렉토리가 존재하는지 Validation
     * @param outputPath output
     * @return
     */
    private boolean isAccessableDir(String outputPath) {
        File outputFile = new File(outputPath);

        return outputFile.exists() && outputFile.isDirectory();
    }

    private boolean isNotAccessableDir(String outputPath) {
        return !isAccessableDir(outputPath);
    }

    /**
     * Partition 숫자로 들어온 값이 숫자인지, 유요한 범위인지 Validation
     * @param numberOfPartition
     * @return
     */
    private boolean isValidPartitionNum(String numberOfPartition) {

        int nPartition;

        try {
            nPartition = Integer.valueOf(numberOfPartition);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        }

        if (nPartition <= 1 || nPartition >= 28)
            return false;

        return true;

    }

    private boolean isNotValidPartitionNum(String numberOfPartition) {
        return !isValidPartitionNum(numberOfPartition);
    }
}
