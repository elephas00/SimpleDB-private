package simpledb.storage;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    
    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;

    private RecordId recordId;

    private Field[] fieldArray;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tupleDesc = td;
        fieldArray = new Field[td.numFields()];
    }

    public static Tuple getInstance(TupleDesc td){
        return new Tuple(td);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        fieldArray[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        return fieldArray[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getField(0));
        for(int i = 1; i < tupleDesc.numFields(); i++){
            builder.append("\t")
                    .append(getField(i));
        }
        return builder.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        List<Field> listField = new ArrayList<>(tupleDesc.numFields());
        for(int i = 0; i < tupleDesc.numFields(); i++){
            listField.set(i, fieldArray[i]);
        }
        return listField.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        tupleDesc = td;
        fieldArray = new Field[td.numFields()];
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }
        if(o instanceof Tuple){
            Tuple tupObj = (Tuple) o;
            if(!this.tupleDesc.equals(tupObj.tupleDesc)){
                return false;
            }
            int n = tupleDesc.numFields();
            for(int i = 0; i < n; i++){
                if(this.fieldArray[i] == null && tupObj.fieldArray[i] == null){
                    continue;
                }
                if(this.fieldArray[i] == null || tupObj.fieldArray[i] == null){
                    return false;
                }
                if(!this.fieldArray[i].equals(tupObj.fieldArray[i])){
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
