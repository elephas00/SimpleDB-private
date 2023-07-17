package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;


import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    
    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private final int aggregateField;

    private final int groupByField;

    private final Aggregator.Op operator;

    private final TupleDesc tupleDesc;

    private final Aggregator aggregator;

    private OpIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        aggregateField = afield;
        groupByField = gfield;
        operator = aop;
        tupleDesc = calcTupleDesc();
        Type groupByFieldType;
        if(Aggregator.NO_GROUPING == groupByField){
            groupByFieldType = null;
        }else{
            groupByFieldType = child.getTupleDesc().getFieldType(groupByField);
        }
        Type aggregateFieldTYpe = child.getTupleDesc().getFieldType(afield);
        if (Type.INT_TYPE.equals(aggregateFieldTYpe)) {
            aggregator = IntegerAggregator.getInstance(aop, gfield, aggregateField, groupByFieldType, this);
        } else if (Type.STRING_TYPE.equals(aggregateFieldTYpe)) {
            aggregator = StringAggregator.getInstance(aop, gfield, aggregateField, groupByFieldType, this);
        } else {
            throw new IllegalStateException("impossible to reach here.");
        }
    }

    private TupleDesc calcTupleDesc() {
        if (Aggregator.NO_GROUPING == groupByField) {
            Type[] typeAr = new Type[]{child.getTupleDesc().getFieldType(aggregateField)};
            String[] fieldAr = new String[]{aggregateFieldName()};
            return TupleDesc.getInstance(typeAr, fieldAr);
        }
        Type[] typeAr = new Type[]{
                child.getTupleDesc().getFieldType(groupByField),
                child.getTupleDesc().getFieldType(aggregateField)
        };
        String[] fieldAr = new String[]{
                child.getTupleDesc().getFieldName(groupByField),
                aggregateFieldName()
        };
        return TupleDesc.getInstance(typeAr, fieldAr);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (Aggregator.NO_GROUPING == groupByField) {
            return null;
        }
        return child.getTupleDesc().getFieldName(groupByField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(aggregateField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        iterator = aggregator.iterator();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void close() {
        super.close();
        child.close();
        iterator.close();
        iterator = null;

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
