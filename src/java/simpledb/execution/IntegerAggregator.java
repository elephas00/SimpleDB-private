package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.io.Serial;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;

    private final int groupByField;

    private final int aggregateField;

    private final Type groupByFieldType;

    /**
     * a map to restore the group by field and the aggregate filed that merged in this aggregation operator.
     * this map should be use when aggregator has a group by clause.
     */
    private Map<Field, List<Field>> groupByMap;

    /**
     * a list to store the aggregate field that merged in this aggregation operator.
     * this list should be use when aggregator have no group by clause.
     * after aggregation, there will only one tuple left.
     */
    private final Aggregator.Op aggregationOperator;

    List<Field> noGroupList;

    private OpIterator child;

    private Aggregate aggregate;

    /**
     * the tuple description of the aggregation result.
     * (GroupByType, INT_TYPE) if there is a group by clause.
     * (INT_TYPE) if there is no group by clause.
     */
    private final TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        groupByField = gbfield;
        aggregateField = afield;
        groupByFieldType = gbfieldtype;
        aggregationOperator = what;
        if (NO_GROUPING == gbfield) {
            noGroupList = new LinkedList<>();
            tupleDesc = TupleDesc.getInstance(new Type[]{Type.INT_TYPE});
        } else {
            groupByMap = new HashMap<>();
            tupleDesc = TupleDesc.getInstance(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * getInstance method, to new a instance of integer aggregator class.
     * @param aop              aggregation operator
     * @param groupByField      the group by field of tuple, may be -1 means there is no group clause.
     * @param aggregateField    the aggregate field of tuple.
     * @param aggregate         the aggregate operator, invoker of this aggregation operator.
     * @return                 a instance of integer aggregator.
     */
    public static IntegerAggregator getInstance(Op aop, int groupByField, int aggregateField, Aggregate aggregate) {
        IntegerAggregator instance;
        if (NO_GROUPING == groupByField) {
            instance = new IntegerAggregator(groupByField, null, aggregateField, aop);
        } else {
            instance = new IntegerAggregator(groupByField, null, aggregateField, aop);
        }
        instance.aggregate = aggregate;
        return instance;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // merge tuples when no group by clause.
        if (NO_GROUPING == groupByField) {
            noGroupList.add(tup.getField(aggregateField));
            return;
        }
        // merge tuples when there exists group by clause.
        Field key = tup.getField(groupByField);
        Field value = tup.getField(aggregateField);
        List<Field> valueList = groupByMap.get(key);
        if (valueList == null) {
            valueList = new LinkedList<>();
            groupByMap.put(key, valueList);
        }
        valueList.add(value);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        return new IntegerAggregatorOpIterator(this);
    }

    /**
     * a help class to generate a OpIterator of a aggregator.
     */
    private static class IntegerAggregatorOpIterator extends Operator {
        /**
         * the invoker of this iterator.
         */
        private final IntegerAggregator aggregator;

        /**
         * the tuple description of the aggregation result,
         * pass in with aggregator, the invoker of this iterator.
         */
        private final TupleDesc tupleDesc;

        /**
         * the result of extends Operator, redundant attribute.
         * likewise, the set children and get children method also redundant.
         */
        private OpIterator child;

        /**
         * list that stores aggregation results.
         */
        private final List<Tuple> tupleList;

        /**
         * the iterator of tupleList.
         */
        private Iterator<Tuple> it;

        /**
         * Constructor, aggregator is the invoker of this iterator,
         * all data need to aggregate are store in the aggregator, the invoker.
         *
         * @param aggregator a integer aggregator, the invoker.
         */
        public IntegerAggregatorOpIterator(IntegerAggregator aggregator) {
            this.aggregator = aggregator;
            this.tupleDesc = aggregator.tupleDesc;
            tupleList = new LinkedList<>();
            if (NO_GROUPING == aggregator.groupByField) {
                computeNoGrouping();
            } else {
                computeGrouping();
            }
            it = tupleList.iterator();
        }

        /**
         * compute if there is a group by clause.
         */
        private void computeGrouping() {
            aggregator.groupByMap.forEach((key, valueList) -> {
                Tuple newTuple = Tuple.getInstance(tupleDesc);
                IntField field = Aggregator.computeAggregating(valueList, aggregator.aggregationOperator);
                newTuple.setField(0, key);
                newTuple.setField(1, field);
                tupleList.add(newTuple);
            });
        }

        /**
         * compute aggregate result when there is no group by clause.
         */
        private void computeNoGrouping() {
            Tuple newTuple = Tuple.getInstance(tupleDesc);
            IntField field = Aggregator.computeAggregating(aggregator.noGroupList, aggregator.aggregationOperator);
            newTuple.setField(0, field);
            tupleList.add(newTuple);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it = tupleList.iterator();
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if (it.hasNext()) {
                return it.next();
            }
            return null;
        }

        @Override
        public OpIterator[] getChildren() {
            return new OpIterator[]{child};
        }

        @Override
        public void setChildren(OpIterator[] children) {
            child = children[0];
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }
    }
}
