package com.jaesang.assignment.partitioner;

/**
 * Ascii code 기반 파티셔너
 */
public class AsciiCodePartitioner extends Partitioner {

    private static String NUMBER = "number";
    private static char NUMBER_MAGIC_CHAR = '{';

    private int partitionNum;

    public AsciiCodePartitioner(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    /**
     * 아스키코드 값 기반으로 Mod 연산하여 Partition 선정
     * @param key a
     * @return
     */
    @Override
    public int getPartition(String key) {
        if (key == null)
            throw new RuntimeException("partition key is null");

        return getAsciiCode(key) % partitionNum;
    }

    /**
     * 해당 파티셔너의 파티션 개수
     * @return
     */
    @Override
    public int getPartitionNum() {
        return this.partitionNum;

    }

    /**
     * Key값에 대한 Ascii code값으로 변경
     * Key값인 number인 경우, z의 다음 ascii값이 '{'로 변환하여 진행
     * @param key number
     * @return 123
     */
    private int getAsciiCode(String key) {
        if (key.equals(NUMBER))
            return (int) NUMBER_MAGIC_CHAR;

        return (int) key.charAt(0);
    }
}
