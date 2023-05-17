package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByField;

    private final int aggregateField;

    private final Type groupByFieldType;
    private Map<Field, List<Field>> groupByMap;

    private final Aggregator.Op aggregationOperator;

    List<Field> noGroupList;

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
        if(NO_GROUPING == gbfield){
            noGroupList = new LinkedList<>();
        }else{
            groupByMap = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // merge tuples when no group by clause.
        if(NO_GROUPING == groupByField){
            noGroupList.add(tup.getField(aggregateField));
            return;
        }
        // merge tuples when there exists group by clause.
        Field key = tup.getField(groupByField);
        Field value = tup.getField(aggregateField);
        List<Field> valueList = groupByMap.get(key);
        if(valueList == null){
            valueList = new LinkedList<>();
            groupByMap.put(key, valueList);
        }
        valueList.add(value);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntegerAggregatorOpIterator(this);
    }

    private static class IntegerAggregatorOpIterator extends Operator{
        private IntegerAggregator aggregator;

        private TupleDesc tupleDesc;

        public IntegerAggregatorOpIterator(IntegerAggregator aggregator){
            this.aggregator = aggregator;
            if(NO_GROUPING == aggregator.groupByField){
                computeNoGrouping();
            }else{
                computeGrouping();
            }
        }

        private void computeGrouping(){
            aggregator.groupByMap.forEach((key, valueList)->{
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
        private void computeNoGrouping(){
            Tuple newTuple = Tuple.getInstance(tupleDesc);
            IntField field = Aggregator.computeAggregating(aggregator.noGroupList, aggregator.aggregationOperator);
            newTuple.setField(0, field);
            tupleList.add(newTuple);
        }

        private List<Tuple> tupleList;

        private Iterator<Tuple> it;

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it = tupleList.iterator();
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if(it.hasNext()){
                return it.next();
            }
            return null;
        }

        @Override
        public OpIterator[] getChildren() {
            // TODO
        }

        @Override
        public void setChildren(OpIterator[] children) {
            // TODO
        }

        @Override
        public TupleDesc getTupleDesc() {
            // TODO
        }
    }
}
