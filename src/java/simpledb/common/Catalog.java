package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {


    /**
     * The index to find table item by table id.
     */
    private final Map<Integer, TableItem> tableId2TableItem;

    /**
     * The index to find table item by table name.
     */
    private final Map<String, TableItem> tableName2TableItem;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        tableId2TableItem = new ConcurrentHashMap<>();
        tableName2TableItem = new ConcurrentHashMap<>();
    }

    /**
     * A help class to manage table.
     */
    public static class TableItem implements Serializable {
        
        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * The dbfile of table item.
         */
        public final DbFile dbFile;

        /**
         * The name of the table item.
         */
        public final String name;

        /**
         * The primary key of the table item.
         */
        public final String primaryKey;

        public TableItem(DbFile f, String n, String p){
            dbFile = f;
            name = n;
            primaryKey = p;
        }

        public static TableItem getInstance(DbFile dbFile, String name, String primaryKey){
            return new TableItem(dbFile, name, primaryKey);
        }
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        TableItem item = TableItem.getInstance(file, name, pkeyField);
        tableId2TableItem.put(file.getId(), item);
        tableName2TableItem.put(name, item);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        TableItem item = getTableByName(name);
        return item.dbFile.getId();
    }

    /**
     * Help function to check whether a table with given name exists.
     * @param name      the table name to query.
     * @return          true if table with given name exists, otherwise false.
     */
    private boolean isExistTableName(String name)  {
        return tableName2TableItem.get(name) != null;
    }

    /**
     * Help function to check whether a table with given table id exists.
     * @param id        the table id to query.
     * @return          true if table with given id exists, otherwise false.
     */
    private boolean isExistTableId(int id){
        return tableId2TableItem.get(id) != null;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        TableItem item = getByTableId(tableid);
        return item.dbFile.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        TableItem item = getByTableId(tableid);
        return item.dbFile;
    }

    public String getPrimaryKey(int tableid) {
        TableItem item = getByTableId(tableid);
        return item.primaryKey;
    }

    public Iterator<Integer> tableIdIterator() {
        return tableId2TableItem.keySet().iterator();
    }

    /**
     * Help function to get a table item by table id.
     * @param tableId       tableId of given table to search.
     * @return              TableItem object with given table id.
     * @throws NoSuchElementException   table not found with given table id.
     */
    private TableItem getByTableId(int tableId) throws NoSuchElementException{
        if(!isExistTableId(tableId)){
            throw new NoSuchElementException();
        }
        return tableId2TableItem.get(tableId);
    }
    /**
     * Help function to get a table item by name.
     * @param name          name of given table to search.
     * @return              TableItem object with given name.
     * @throws NoSuchElementException   table not found with given name.
     */
    private TableItem getTableByName(String name) throws NoSuchElementException{
        if(!isExistTableName(name)){
            throw new NoSuchElementException();
        }
        return tableName2TableItem.get(name);
    }
    public String getTableName(int id) {
        TableItem item = getByTableId(id);
        return item.name;
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        tableId2TableItem.clear();
        tableName2TableItem.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                List<String> names = new ArrayList<>();
                List<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

