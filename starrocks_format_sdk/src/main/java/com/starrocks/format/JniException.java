package com.starrocks.format;

public class JniException extends Exception {
    private final int errorCode;

    public JniException(int code, String message) {
        super(message);
        this.errorCode = code;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
