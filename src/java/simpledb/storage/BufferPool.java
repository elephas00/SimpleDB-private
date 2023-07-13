package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static simpledb.common.Database.getCatalog;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final List<Page> pageList;

    private final int pageNum;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pageNum = numPages;
        pageList = new CopyOnWriteArrayList<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be locked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // traversal the page list to find page.
        boolean acquiredLock = Database.getLockManager().lockPage(tid, pid, perm);
        if(!acquiredLock){
            throw new TransactionAbortedException("failed to acquire lock for page " + pid);
        }
        Page target = null;
        for(Page page : pageList){
            if(page != null && pid.equals(page.getId())){
                target = page;
                break;
            }
        }
        if(target == null){
            if(pageList.size() >= pageNum){
                evictPage();
            }
            Page loadPage = Database.getCatalog().getTable(pid.getTableId()).readPage(pid);
            pageList.add(loadPage);
            target = loadPage;
        }
        return target;
    }

    /**
     * Releases the lock on a page.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        Database.getLockManager().unlockPage(tid, pid);

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        this.transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return Database.getLockManager().holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if(commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                Thread.currentThread().interrupt();
            }
        }else{
            Set<Page> rollbackPages = pageList.stream()
                    .filter(page -> Database.getLockManager().holdsLock(tid, page.getId()) && tid.equals(page.isDirty()))
                    .collect(Collectors.toSet());
            pageList.removeAll(rollbackPages);
        }


        Database.getLockManager().unlockAllPages(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        DbFile dbFile = getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);

        dirtyPages.forEach(page -> page.markDirty(true, tid));

        for (Page dirtyPage : dirtyPages) {
            if (pageList.contains(dirtyPage)) {
                continue;
            }
            if (pageList.size() == pageNum) {
                evictPage();
            }
            dbFile.writePage(dirtyPage);
            pageList.add(dirtyPage);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        RecordId recordId = t.getRecordId();
        int tableId = recordId.getPageId().getTableId();
        DbFile dbFile = getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        dirtyPages.forEach(page -> page.markDirty(true, tid));

        for (Page dirtyPage : dirtyPages) {
            if (pageList.contains(dirtyPage)) {
                continue;
            }
            if (pageList.size() == pageNum) {
                evictPage();
                pageList.add(dirtyPage);
            }
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : pageList) {
            flushPage(page.getId());
        }
        Database.getLockManager().releaseAllLocks();
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        if(pid == null){
            return;
        }
        boolean removed = pageList.removeIf(page -> pid.equals(page.getId()));
//        if(!removed){
//            throw new RuntimeException("remove page failed, page not in buffer pool");
//        }

    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        int tableId = pid.getTableId();
        DbFile dbFile = getCatalog().getTable(tableId);
        Page page = null;
        try {
            page = getPage(null, pid, Permissions.READ_WRITE);
        } catch (TransactionAbortedException e) {
            throw new RuntimeException(e);
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
        dbFile.writePage(page);

    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        Set<Page> dirtyPages = pageList.stream()
                .filter(page -> Database.getLockManager().holdsLock(tid, page.getId()))
                .filter(page -> page.isDirty() == tid)
                .collect(Collectors.toSet());

        for (Page page : dirtyPages){
            flushPage(page.getId());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // FIFO strategy, remove first non-dirty page in buffer poll.
        for (Page page : pageList) {
            if(page.isDirty() == null){
                removePage(page.getId());
                return;
            }
        }
        throw new DbException("evict page failed, buffer pool is full with dirty page.");
    }

}
