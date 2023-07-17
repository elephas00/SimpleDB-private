package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.IOException;


/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    
    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;

    private OpIterator child;

    private boolean isExecute;

    private static final TupleDesc tupleDesc;
    static {
        Type[] typeAr = new Type[]{ Type.INT_TYPE };
        tupleDesc = TupleDesc.getInstance(typeAr);
    }


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        transactionId = t;
        this.child = child;
        isExecute = false;
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
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(isExecute){
            return null;
        }
        isExecute = true;
        int count = 0;
        try{
            while(child.hasNext()){
                Tuple next = child.next();
                int tableId = next.getRecordId().getPageId().getTableId();
                DbFile dbFile = Database.getCatalog().getTable(tableId);
                dbFile.deleteTuple(transactionId, next);
                count++;
            }
        }catch (IOException e){
            throw new DbException("delete failed " + e.getMessage());
        }
        Tuple res = Tuple.getInstance(tupleDesc);
        IntField field = IntField.getInstance(count);
        res.setField(0, field);
        return res;

    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{ child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
