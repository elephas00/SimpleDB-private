package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] buf;
        if (Database.getCatalog().getTableName(pid.getTableId()) == null) {
            throw new IllegalArgumentException();
        }
        try {
            InputStream is = new FileInputStream(file);
            buf = new byte[BufferPool.getPageSize()];
            is.skip(offset);
            is.read(buf);
            is.close();
            return HeapPage.getInstance(
                    HeapPageId.getInstance(pid.getTableId(), pid.getPageNumber()),
                    buf
            );
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e1) {
            throw new RuntimeException();
        }
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        final int pageNo = page.getId().getPageNumber();
        final int pageSize = BufferPool.getPageSize();
        final int offset = pageSize * pageNo;
        final byte[] buf = page.getPageData();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.skipBytes(offset);
        raf.write(buf);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) file.length() / BufferPool.getPageSize();
    }


    private Page findPageWithEmptySlot(TransactionId tid) throws TransactionAbortedException, DbException, IOException {
        final int numPages = numPages();
        Page res = null;
        for (int i = 0; i < numPages; i++) {
            HeapPageId pid = HeapPageId.getInstance(getTableId(), i);
            boolean holdsLock = Database.getBufferPool().holdsLock(tid, pid);
            Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            HeapPage heapPage = (HeapPage) page;
            if(heapPage.getNumUnusedSlots() > 0){
                res = heapPage;
                break;
            }else if(!holdsLock){
                Database.getLockManager().unlockPage(tid, pid);
            }
        }
        if (res == null) {
            // need a new page.
            HeapPageId pid = HeapPageId.getInstance(getTableId(), numPages);
            HeapPage emptyPage = HeapPage.getEmptyInstance(pid);
            writePage(emptyPage);
            res =  Database.getBufferPool().getPage(tid, emptyPage.getId(), Permissions.READ_ONLY);
        }
        return res;
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        Page page = findPageWithEmptySlot(tid);
        Page readWritePage = Database.getBufferPool().getPage(tid, page.getId(), Permissions.READ_WRITE);
        HeapPage dirtyPage = (HeapPage) readWritePage;
        dirtyPage.insertTuple(t);
        dirtyPage.markDirty(true, tid);
        List<Page> dirtyPages = new LinkedList<>();
        dirtyPages.add(dirtyPage);
        return dirtyPages;
    }

    private int getTableId() {
        return file.getAbsolutePath().hashCode();
    }

    // see DbFile.java for javadocs
   @Override
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        HeapPage heapPage = (HeapPage) page;
        heapPage.deleteTuple(t);
        heapPage.markDirty(true, tid);
        List<Page> dirtyPages = new LinkedList<>();
        dirtyPages.add(heapPage);
        return dirtyPages;
    }

//    private void appendFileLengthOnDisk(byte[] buf) throws IOException {
//        FileOutputStream fo = new FileOutputStream(file, true);
//        fo.write(buf);
//    }
//    private void appendFileLengthOnDisk(int extendLength) throws IOException, TransactionAbortedException, DbException {
//        FileOutputStream fo = new FileOutputStream(file, true);
//        byte[] buf = new byte[extendLength];
//        fo.write(buf);
//    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return DbHeapFileIterator.getInstance(this, tid);
    }

    public static class DbHeapFileIterator extends AbstractDbFileIterator {
        private boolean isOpen;
        private final HeapFile dbFile;
        private HeapPage curPage;
        private int curPageNo;
        public Iterator<Tuple> iteratorInCurrentPage;
        private final TransactionId transactionId;

        private DbHeapFileIterator(HeapFile heapfile, TransactionId tid) {
            dbFile = heapfile;
            isOpen = false;
            transactionId = tid;
        }

        public static DbHeapFileIterator getInstance(HeapFile heapFile, TransactionId transactionId) {
            return new DbHeapFileIterator(heapFile, transactionId);
        }


        private void readCurrentPageFromBufferPoll() throws TransactionAbortedException, DbException {
            HeapPageId pageId = HeapPageId.getInstance(dbFile.getTableId(), curPageNo);
            curPage = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
            iteratorInCurrentPage = curPage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }
            if (iteratorInCurrentPage.hasNext()) {
                return true;
            }
            if (curPageNo == dbFile.numPages() - 1) {
                return false;
            }
            curPageNo++;
            readCurrentPageFromBufferPoll();
            return hasNext();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException, NoSuchElementException{
            checkIteratorOpen();
            if (hasNext()) {
                return iteratorInCurrentPage.next();
            }
            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            curPageNo = 0;
            isOpen = true;
            readCurrentPageFromBufferPoll();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            checkIteratorOpen();
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            isOpen = false;
        }

        private void checkIteratorOpen() throws NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException("iterator was closed.");
            }
        }
    }
}

