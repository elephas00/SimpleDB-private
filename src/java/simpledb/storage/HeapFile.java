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
    public int getId() {
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
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

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        final int numPages = numPages();
        boolean needNewPage = true;
        Page dirtyPage = null;
        for (int i = 0; i < numPages; i++) {
            HeapPageId pid = HeapPageId.getInstance(getTableId(), i);
            Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            HeapPage heapPage = (HeapPage) page;
            if (insertTupleToPage(heapPage, t, tid)) {
                needNewPage = false;
                dirtyPage = heapPage;
                break;
            }
        }
        if (needNewPage) {
            HeapPageId pid = HeapPageId.getInstance(getTableId(), numPages);
            HeapPage emptyPage = HeapPage.getEmptyInstance(pid);
            insertTupleToPage(emptyPage, t, tid);
            dirtyPage = emptyPage;
            writePage(emptyPage);
        }
        List<Page> dirtyPages = new LinkedList<>();
        dirtyPages.add(dirtyPage);
        return dirtyPages;
    }

    /**
     * A help function to insert a tuple to a given page.
     * If the page is full, return false.
     * If the page is not full, insert tuple tup to this page.
     *
     * @param heapPage page id of given page.
     * @param tup      given tuple
     * @param tid      given transaction id.
     * @return true if insertion is success.
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private boolean insertTupleToPage(HeapPage heapPage, Tuple tup, TransactionId tid) throws DbException, TransactionAbortedException {
        if (heapPage.getNumUnusedSlots() > 0) {
            heapPage.insertTuple(tup);
            heapPage.markDirty(true, tid);
            return true;
        }
        return false;
    }

    private int getTableId() {
        return file.getAbsolutePath().hashCode();
    }

    // see DbFile.java for javadocs
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


        private void readCurrentPageFromBufferPoll() {
            HeapPageId pageId = HeapPageId.getInstance(dbFile.getTableId(), curPageNo);
            try {
                curPage = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
            } catch (TransactionAbortedException | DbException | ClassCastException e) {
                throw new RuntimeException("read next page failed.", e);
            }
            iteratorInCurrentPage = curPage.iterator();
        }

        @Override
        public boolean hasNext() {
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
        protected Tuple readNext() {
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

