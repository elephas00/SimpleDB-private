package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate predicate;

    private final OpIterator child;

    private boolean isOpen;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    private void checkFilterIsOpen() throws DbException{
        if(!isOpen){
            throw new DbException("this filter is closed. ");
        }
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        isOpen = true;
    }

    public void close() {
        isOpen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        checkFilterIsOpen();
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        checkFilterIsOpen();
        // TODO: some code goes here
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
    }

}
