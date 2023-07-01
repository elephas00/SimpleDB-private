package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManagerImpl implements LockManager{
    @Override
    public boolean lockPage(TransactionId tid, PageId pid, Permissions perm) {
        return true;
    }

    @Override
    public boolean unlockPage(TransactionId tid, PageId pid) {
        return true;
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return true;
    }

    @Override
    public boolean unlockAllPages(TransactionId transactionId) {
        return false;
    }
}
