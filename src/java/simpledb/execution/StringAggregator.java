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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    
    private static final long serialVersionUID = 1L;

    private final int aggregateFiled;

    private final int groupByFiled;

    private final Type groupByType;

    private final Op aggregationOperator;

    List<Field> noGroupByList;

    Map<Field, List<Field>> groupByMap;

    private final TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(!Op.COUNT.equals(what)){
            throw new IllegalStateException("string aggregator only support COUNT function.");
        }
        aggregationOperator = what;
        groupByFiled = gbfield;
        aggregateFiled = afield;
        groupByType = gbfieldtype;
        if(NO_GROUPING == gbfield){
            noGroupByList = new LinkedList<>();
            tupleDesc = TupleDesc.getInstance(new Type[]{Type.INT_TYPE});
        }else{
            groupByMap = new HashMap<>();
            tupleDesc = TupleDesc.getInstance(new Type[]{groupByType, Type.INT_TYPE});
        }
    }

    public static StringAggregator getInstance(Op aop, int groupByField, int aggregateField, Type groupByType, Aggregate aggregate) {
        if(NO_GROUPING == groupByField){
            return new StringAggregator(groupByField, null, aggregateField, aop);
        }else{
            return new StringAggregator(groupByField, groupByType, aggregateField, aop);
        }

    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(NO_GROUPING == groupByFiled){
            noGroupByList.add(tup.getField(aggregateFiled));
            return;
        }
        Field key = tup.getField(groupByFiled);
        Field value = tup.getField(aggregateFiled);
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
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return StringAggregatorOpIterator.getInstance(this);
    }

    private static class StringAggregatorOpIterator extends Operator{

        private final StringAggregator aggregator;
        private final List<Tuple> tupleList;

        private Iterator<Tuple> it;

        private StringAggregatorOpIterator(StringAggregator ag){
            aggregator = ag;
            tupleList = new LinkedList<>();
            if(!Op.COUNT.equals(ag.aggregationOperator)){
                throw new IllegalStateException("illegal aggregation operator.");
            }
            if(NO_GROUPING == aggregator.aggregateFiled){
                Tuple newTuple = Tuple.getInstance(ag.tupleDesc);
                IntField f = Aggregator.computeAggregating(ag.noGroupByList, ag.aggregationOperator);
                newTuple.setField(0, f);
                tupleList.add(newTuple);
            }else{
                ag.groupByMap.forEach((key, valueList) ->{
                    Tuple newTuple = Tuple.getInstance(ag.tupleDesc);
                    IntField f = Aggregator.computeAggregating(valueList, ag.aggregationOperator);
                    newTuple.setField(0, key);
                    newTuple.setField(1, f);
                    tupleList.add(newTuple);
                });
            }
            it = tupleList.iterator();
        }

        public static StringAggregatorOpIterator getInstance(StringAggregator aggregator){
            return new StringAggregatorOpIterator(aggregator);
        }
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
            return new OpIterator[0];
        }

        /**
         *
         * @param children the DbIterators which are to be set as the children(child) of
         *                 this operator
         */
        @Override
        public void setChildren(OpIterator[] children) {

        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.tupleDesc;
        }
    }
}
