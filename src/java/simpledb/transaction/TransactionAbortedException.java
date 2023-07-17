package simpledb.transaction;

/**
 * Exception that is thrown when a transaction has aborted.
 */
public class TransactionAbortedException extends Exception {
    private static final long serialVersionUID = 1L;

    public TransactionAbortedException() {
    }

    public TransactionAbortedException(String msg, Throwable e) {
        super(msg, e);
    }

    public TransactionAbortedException(String msg) {
        this(msg, null);
    }
}
