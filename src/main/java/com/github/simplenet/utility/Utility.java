package com.github.simplenet.utility;

public final class Utility {

    private Utility() {
        throw new UnsupportedOperationException("This class cannot be instantiated!");
    }

    public static int roundUpToNextMultiple(int num, int multiple) {
        if (multiple == 0) {
            return num;
        }
        return num + multiple - (num % multiple);
    }
}