package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

public interface LockManager {
    /**
     * lock a page for a transaction
     * @param tid   transaction id
     * @param pid   page id
     * @param perm  permission
     * @return
     */
    boolean lockPage(TransactionId tid, PageId pid, Permissions perm);

    /**
     * unlock a page for a transaction
     * @param tid   transaction id
     * @param pid   page id
     * @return
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

}

