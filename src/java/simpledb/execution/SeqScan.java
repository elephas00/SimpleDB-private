package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.StringUtils;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private boolean isOpen;

    private TransactionId transactionId;

    private int tableId;

    private String tableAlias;

    private DbFileIterator iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        transactionId = tid;
        tableId = tableid;
        this.tableAlias = tableAlias;
        isOpen = false;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        checkState();
        tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        isOpen = true;
        iterator = HeapFile.DbHeapFileIterator.getInstance(
                (HeapFile) Database.getCatalog().getTable(tableId),
                transactionId
        );
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc origin = Database.getCatalog().getTupleDesc(tableId);
        if(StringUtils.isBlank(tableAlias)){
            return origin;
        }
        return TupleDesc.getAliasWrappedInstance(origin, tableAlias);

    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        checkState();
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        checkState();
        return iterator.next();
    }

    public void close() {
        isOpen = false;
        iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        checkState();
        iterator.rewind();
    }

    private void checkState() throws IllegalArgumentException{
        if(!isOpen){
            throw new IllegalArgumentException("SeqScan op is closed.");
        }
    }
}
