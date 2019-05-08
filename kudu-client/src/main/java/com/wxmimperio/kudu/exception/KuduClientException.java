package com.wxmimperio.kudu.exception;

public class KuduClientException extends Exception {

    public KuduClientException() {
        super();
    }

    public KuduClientException(String message) {
        super(message);
    }

    public KuduClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
