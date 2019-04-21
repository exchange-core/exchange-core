package org.openpredict.exchange.core;

public final class BinaryUtils {

    public static int requiredLongArraySize(final int bytesLength) {
        return ((bytesLength - 1) >> 3) + 1;
    }

}
