package org.openpredict.exchange.rdma;

public class RdmaApiConstants {

    public static final int CMD_HEADER = 0; // symbol (bits 63-32) + subCommandType (bits 14-8) + commandType + (6-0)

    public static final int CMD_TIMESTAMP = 1;
    public static final int CMD_UID = 2;
    public static final int CMD_ORDER_ID = 3;
    public static final int CMD_PRICE = 4;
    public static final int CMD_SIZE = 5;
    public static final int CMD_PLACEORDER_FLAGS = 6;
    // word 6: for Place Order : [4 bytes cookie] [][][type byte][action byte]

}
