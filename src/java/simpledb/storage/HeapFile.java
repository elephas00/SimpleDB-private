package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
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

    /**
      Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */

    private final File file;

    private final TupleDesc tupleDesc;

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
        long offset = pid.getPageNumber()*BufferPool.getPageSize();
//        Debug.log("file length:" + file.length() +"; offset:" + offset + "; page size:" + BufferPool.getPageSize());
//        Debug.log(offset > Integer.MAX_VALUE ? "offset overflow" : "offset valid");
//        Debug.log("read page " + pid.getPageNumber());
        byte[] buf;
        if(Database.getCatalog().getTableName(pid.getTableId()) == null){
            throw new IllegalArgumentException();
        }
        try {
            InputStream is = new FileInputStream(file);
            buf = new byte[BufferPool.getPageSize()];
//            is.read(buf);
//            is.read(buf, 0, BufferPool.getPageSize());
//            if(offset + BufferPool.getPageSize() >= file.length()){
//                is.skip(offset);
//                is.read(buf);
//            }else{
//                is.read(buf, (int) offset, BufferPool.getPageSize());
//            }
            is.skip(offset);
            is.read(buf);
            is.close();
            HeapPage res = HeapPage.getInstance(
                    HeapPageId.getInstance(pid.getTableId(), pid.getPageNumber()),
                    buf
            );
            return res;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }catch (IOException e1){
            throw new RuntimeException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(1.0 * file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        for(int i = 0; i < numPages(); i++){
            HeapPageId pid = HeapPageId.getInstance(getTableId(), i);
            Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            HeapPage heapPage = (HeapPage) page;
            if(heapPage.getNumUnusedSlots() > 0){
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                // TODO: some code goes here
                return null;
            }
        }

        // TODO: some code goes here
        return null;

    }

    private int getTableId(){
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
        // TODO: what will return here?
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return DbHeapFileIterator.getInstance(this, tid);
    }

    public static class DbHeapFileIterator extends AbstractDbFileIterator{
        private boolean isOpen;
        private HeapFile file;
        private TransactionId tid;
        private int pageNumber;

        private Iterator<Tuple> tupleIterator;
        private DbHeapFileIterator(HeapFile heapFile, TransactionId tid){
            isOpen = false;
            file = heapFile;
            this.tid = tid;
            close();
        }
        public static DbHeapFileIterator getInstance(HeapFile heapFile, TransactionId tid){
            return new DbHeapFileIterator(heapFile, tid);
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException {
            if(!isOpen){
                return false;
            }
            if(file.numPages() == 0){
                return false;
            }
            if(pageNumber == file.numPages()){
                return false;
            }
            if(tupleIterator == null){
                pageNumber = -1;
                readNextPage();
                return this.hasNext();
            }
            if(!tupleIterator.hasNext()){
                if(hasNextPage()){
                    readNextPage();
                    return this.hasNext();
                }
                return false;
            }
            return true;
        }

        private void readNextPage() throws TransactionAbortedException, DbException {
            pageNumber++;
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, HeapPageId.getInstance(file.getId(), pageNumber), Permissions.READ_ONLY);
            tupleIterator = page.iterator();
        }
        private boolean hasNextPage(){
            int numpages = file.numPages();
            if(pageNumber + 1 >= numpages){
                return false;
            }
            return true;
        }

        private void checkIteratorOpen() throws NoSuchElementException {
            if(!isOpen){
                throw new NoSuchElementException("iterator was closed.");
            }
        }
        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            checkIteratorOpen();
            if(hasNext()){
                return tupleIterator.next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            tupleIterator = null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            checkIteratorOpen();
            tupleIterator = null;
        }

        @Override
        public void close(){
            isOpen = false;
        }
    }
}

