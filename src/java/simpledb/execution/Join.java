package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private final JoinPredicate predicate;

    private final OpIterator child1;

    private final OpIterator child2;

    private Tuple child1CurrentElement;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return predicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(predicate.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(predicate.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(
                child1.getTupleDesc(),
                child2.getTupleDesc()
        );
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();

    }

    public void close() {
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(child1CurrentElement == null){
            child1CurrentElement = child1.next();
        }
        if(!child2.hasNext()){
            child1CurrentElement = child1.next();
            child2.rewind();
        }
        final Tuple child2CurrentElement = child2.next();
        final Tuple newTuple = Tuple.getInstance(this.getTupleDesc());
        final int c1NumF = child1.getTupleDesc().numFields();
        final int c2NumF = child2.getTupleDesc().numFields();
        for(int i = 0; i < c1NumF; i++){
            newTuple.setField(i, child1CurrentElement.getField(i));
        }
        for(int i = 0; i < c2NumF; i++){
            int posInNewTuple = i + c2NumF;
            newTuple.setField(posInNewTuple, child2CurrentElement.getField(posInNewTuple));
        }
        return newTuple;
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
