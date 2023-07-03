package simpledb.storage;

import java.io.Serial;
import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final PageId pageId;
    private final int tupleNumber;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        pageId = pid;
        tupleNumber = tupleno;
    }

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleNo the tuple number within the page.
     */
    public static RecordId getInstance(PageId pid, int tupleNo){
        return new RecordId(pid, tupleNo);
    }
    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if(this == o){
            return true;
        }
        if(o instanceof RecordId){
            RecordId rIdObj = (RecordId) o;
            return rIdObj.getTupleNumber() == tupleNumber && rIdObj.getPageId().equals(pageId);
        }
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return tupleNumber;
    }

}
