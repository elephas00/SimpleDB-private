package simpledb.storage;

import simpledb.common.StringUtils;
import simpledb.common.Type;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    final Type[] typeArray;
    final String[] fieldArray;
    final List<TDItem> listTDItem;
    final int size;
    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return listTDItem.iterator();
    }

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // a valid tuple description must have at least one filed.
        if(typeAr == null || typeAr.length == 0){
            throw new IllegalArgumentException();
        }
        int len = typeAr.length;
        ArrayList<TDItem> items = new ArrayList<>(len);
        for(int i = 0; i < len; i++){
            items.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        typeArray = typeAr;
        fieldArray = fieldAr;
        listTDItem = items;
        size = calcSize(typeAr);
    }

    /**
     * Calculate the size of given type array.
     * @param typeAr    given type array to calculate.
     * @return          size of given type array.
     */
    private static int calcSize(Type[] typeAr){
        int sum = 0;
        for(Type tp : typeAr){
            sum += tp.getLen();
        }
        return sum;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // a valid tuple description must have at least one filed.
        if(typeAr == null || typeAr.length == 0){
            throw new IllegalArgumentException();
        }
        final int len = typeAr.length;

        ArrayList<TDItem> items = new ArrayList<>(len);
        for(int i = 0; i < len; i++){
            items.add(new TDItem(typeAr[i], null));
        }
        listTDItem = items;
        typeArray = typeAr;
        fieldArray = new String[len];
        size = calcSize(typeAr);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return listTDItem.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i < 0 || i >= numFields()){
            throw new NoSuchElementException();
        }
        return fieldArray[i];
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i < 0 || i >= numFields()){
            throw new NoSuchElementException();
        }
        return typeArray[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        if(StringUtils.isBlank(name)){
            throw new NoSuchElementException();
        }

        for(int i = 0; i < numFields(); i++){
            if(name.equals(fieldArray[i])){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int newNumFields = td1.numFields() + td2.numFields();
        Type[] newTypeAr = new Type[newNumFields];
        String[] newFieldAr = new String[newNumFields];
        // copy types
        System.arraycopy(td1.typeArray, 0, newTypeAr, 0, td1.numFields());
        System.arraycopy(td2.typeArray, 0, newTypeAr, td1.numFields(), td2.numFields());
        // copy fields
        System.arraycopy(td1.fieldArray, 0, newFieldAr, 0, td1.numFields());
        System.arraycopy(td2.fieldArray, 0, newFieldAr, td1.numFields(), td2.numFields());
        return new TupleDesc(newTypeAr, newFieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(!(o instanceof TupleDesc)){
            return false;
        }
        return Arrays.compare(typeArray, ((TupleDesc) o).typeArray) == 0;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(listTDItem.get(0).toString());
        for(int i = 1; i < numFields(); i++){
            builder.append(',')
                    .append(listTDItem.get(i).toString())
                    .append(')');
        }
        return builder.toString();
    }
}
