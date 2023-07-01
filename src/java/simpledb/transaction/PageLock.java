package simpledb.transaction;

import simpledb.storage.PageId;

public interface PageLock {
    /**
     * acquire the shared lock of this page.
     * if current thread is already the owner of shared lock, this method should return ture.
     * if the shared lock and exclusive are both free,
     * current thread must acquire this lock immediately, and this method should return true.
     * if the shared lock is hold by other thread,
     * current thread must acquire this lock immediately, and this method should return true.
     * if the exclusive lock is hold by other thread,
     * current thread must wait until other thread release exclusive or time out.
     * if other thread release exclusive,
     * current thread must acquire the shared lock immediately, and this method should return true.
     * if a time-out event is trigger,
     * current thread will not acquire the shared lock, and this method should return false.
     *
     * @param transactionId transaction id of the transaction will acquire this lock.
     * @return true if the transaction acquires lock, or else false.
     */
    boolean acquireSharedLock(TransactionId transactionId);

    /**
     * acquire the exclusive lock of this page.
     * if current thread is already the owner of shared lock, this method should return ture.
     * if the shared lock and exclusive are both free,
     * current thread must acquire this lock immediately, and this method should return true.
     * if the shared lock is only hold by this thread,
     * current thread must upgrade this shared lock to exclusive lock, and this method should return true.
     * if the shared lock or exclusive lock is hold by other thread,
     * current thread should wait until no thread holds lock or a time-out event trigger,
     * if other threads release lock,
     * current thread should acquire exclusive lock, and this method should return true.
     * if a time-out event trigger,
     * current thread should not acquire exclusive, and this method should return false.
     *
     * @param transactionId transaction id of the transaction will acquire this lock.
     * @return true if the transaction acquires lock, or else false.
     */
    boolean acquireExclusiveLock(TransactionId transactionId);


    /**
     * release the lock of this page.
     *
     * @param transactionId transaction id of the transaction will release this lock.
     * @return true if the transaction releases lock, or else false.
     */
    boolean releaseLock(TransactionId transactionId);

    PageId getPageId();
}
