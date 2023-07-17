package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    
    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;

    private OpIterator child;

    private final int tableId;


    /**
     * the insert returns a one field tuple containing the number inserted records.
     */
    private final static TupleDesc tupleDesc;
    static {
        Type[] typeAr = new Type[]{Type.INT_TYPE};
        tupleDesc = TupleDesc.getInstance(typeAr);
    }

    private boolean isExecuted;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        isExecuted = false;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(isExecuted){
            return null;
        }
        isExecuted = true;
        DbFile dbfile = Database.getCatalog().getTable(tableId);
        int count = 0;
        try{
            while(child.hasNext()){
                Tuple next = child.next();
                dbfile.insertTuple(transactionId, next);
                count++;
            }
        }catch (IOException e){
            throw new DbException("insert failed " + e.getMessage());
        }
        Tuple res = Tuple.getInstance(tupleDesc);
        IntField field = IntField.getInstance(count);
        res.setField(0, field);
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
