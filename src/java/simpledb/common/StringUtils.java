package simpledb.common;

public class StringUtils {
    /**
     * check whether a String object is blank.
     * @param str    String object to check
     * @return       true if str is blank, false if str is not blank
     */
    public static boolean isBlank(String str){
        return null == str || "".equals(str);
    }
}
