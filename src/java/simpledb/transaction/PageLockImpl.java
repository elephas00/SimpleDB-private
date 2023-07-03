package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.HashSet;
import java.util.Set;

public class PageLockImpl implements PageLock {
    /**
     * pageId of this page
     */
    private PageId pageId;

    /**
     * transactionId of the transaction hold this lock
     */
    private Set<TransactionId> holders;

    /**
     * lock type of this read write lock
     */
    private Permissions permission;
    private PageLockImpl(PageId pageId, Set<TransactionId> holders, Permissions permission){
        this.pageId = pageId;
        this.holders = holders;
        this.permission = permission;
    }

    public static PageLockImpl getInstance(PageId pageId){
        return new PageLockImpl(pageId, new HashSet<>(), Permissions.READ_ONLY);
    }

    @Override
    public boolean acquireSharedLock(TransactionId transactionId) {
        synchronized (this){
            if(holders.isEmpty()){
                permission = Permissions.READ_ONLY;
                holders.add(transactionId);
                return true;
            }
            if(holders.contains(transactionId)){
                return true;
            }
            if(Permissions.READ_ONLY.equals(permission)){
                holders.add(transactionId);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean acquireExclusiveLock(TransactionId transactionId) {
        synchronized (this){
            // this lock has no owner.
            if(holders.isEmpty()){
                permission = Permissions.READ_WRITE;
                holders.add(transactionId);
                return true;
            }
            // this transaction is the owner of this exclusive lock.
            if(Permissions.READ_WRITE.equals(permission) && holders.contains(transactionId)){
                return true;
            }
            // upgrade shared lock to exclusive lock.
            if(Permissions.READ_ONLY.equals(permission) && holders.contains(transactionId)){
                if (holders.size() == 1){
                    permission = Permissions.READ_WRITE;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean releaseLock(TransactionId transactionId) {
        synchronized (this){
            if(Permissions.READ_ONLY.equals(permission)){
                holders.remove(transactionId);
                return true;
            }
            if(Permissions.READ_WRITE.equals(permission)){
                holders.remove(transactionId);
                return true;
            }
        }
        return false;
    }

    @Override
    public PageId getPageId() {
        return pageId;
    }
}

