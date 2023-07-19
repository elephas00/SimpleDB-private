package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Set;

public interface LockManager {
    /**
     * lock a page for a transaction
     * @param tid   transaction id
     * @param pid   page id
     * @param perm  permission
     * @return      true if lock acquired
     */
    boolean lockPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException;

    /**
     * unlock a page for a transaction
     * @param tid   transaction id
     * @param pid   page id
     * @return      true if lock released.
     */
    boolean unlockPage(TransactionId tid, PageId pid);


    /**
     * check if given transaction holds a lock on given page.
     * @param tid   id of given transaction
     * @param pid   id of given page
     * @return      true if holds a lock, or else false.
     */
    boolean holdsLock(TransactionId tid, PageId pid);

    /**
     * unlock all page that relative to given transaction.
     * @param transactionId     transaction id.
     * @return                  true if all released, or else false.
     */
    boolean unlockAllPages(TransactionId transactionId);

    /**
     * method for testing when initialize test case, this method
     * should execute after all test pages flush to disk.
     */
    void releaseAllLocks();

    /**
     * check if given page is write locked by any transaction.
     * @param pageId    id of the page to check.
     * @return          true if locked, or else false.
     */
    boolean isWriteLocked(PageId pageId);

    Set<PageId> readWritePages(TransactionId transactionId);
}

