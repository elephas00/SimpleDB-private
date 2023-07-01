package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManagerImpl implements LockManager{
    /**
     * lock table to find all page locks.
     */
    private final Map<PageId, PageLock> lockTable;

    /**
     * lock table to find all locks for specified transaction.
     */
    private final Map<TransactionId, Set<PageLock>> transactionLockTable;

    public static LockManager getInstance() {
        return new LockManagerImpl();
    }

    private LockManagerImpl(){
        lockTable = new ConcurrentHashMap<>();
        transactionLockTable = new ConcurrentHashMap<>();
    }

    @Override
    public boolean lockPage(TransactionId transactionId, PageId pageId, Permissions permission) throws TransactionAbortedException{
        if (transactionId == null) {
            return true;
        }
        PageLock iPageLock = lockTable.get(pageId);
        if (iPageLock == null) {
            iPageLock = PageLockImpl.getInstance(pageId);
            lockTable.put(pageId, iPageLock);
        }
        boolean acquired = false;
        if (Permissions.READ_ONLY.equals(permission)) {
            acquired = iPageLock.acquireSharedLock(transactionId);
        } else if (Permissions.READ_WRITE.equals(permission)) {
            acquired = iPageLock.acquireExclusiveLock(transactionId);
        } else {
            throw new RuntimeException("unknown permission " + permission);
        }
        if (!acquired){
            return false;
        }
        Set<PageLock> locks4xaction = transactionLockTable.get(transactionId);
        if (locks4xaction == null) {
            locks4xaction = new HashSet<>();
            transactionLockTable.put(transactionId, locks4xaction);
        }
        locks4xaction.add(iPageLock);
        return true;
    }
    @Override
    public boolean unlockPage(TransactionId transactionId, PageId pageId) {
        PageLock iPageLock = lockTable.get(pageId);

        boolean released = iPageLock.releaseLock(transactionId);
        if (!released) {
            throw new RuntimeException("failed to release lock for page " + pageId);
        }
        Set<PageLock> locks4Transaction = transactionLockTable.get(transactionId);
        locks4Transaction.remove(pageId);
        return true;
    }


    @Override
    public boolean holdsLock(TransactionId tid, PageId pid) {
        Set<PageLock> holdLocks = transactionLockTable.get(tid);
        if(holdLocks == null){
            return false;
        }
        return holdLocks.stream()
                .map(PageLock::getPageId)
                .anyMatch(holdsLockPageId -> holdsLockPageId.equals(pid));
    }

    @Override
    public boolean unlockAllPages(TransactionId transactionId) {
        Set<PageLock> iPageLocks = transactionLockTable.get(transactionId);
        if (iPageLocks == null){
            return true;
        }
        for (PageLock iPageLock : iPageLocks) {
            boolean released = iPageLock.releaseLock(transactionId);
            if (!released) {
                throw new RuntimeException("failed to release lock for transaction " + transactionId);
            }
        }
        transactionLockTable.put(transactionId, new HashSet<>());
        return true;
    }

    @Override
    public void releaseAllLocks() {
        this.lockTable.clear();
        this.transactionLockTable.clear();
    }

}