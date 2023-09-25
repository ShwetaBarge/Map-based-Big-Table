package global;

public interface GlobalConst {

    int MINIBASE_MAXARRSIZE = 50;
    int NUMBUF = 200;

    /**
     * Size of page.
     */
    int MINIBASE_PAGESIZE = 1024;           // in bytes

    /**
     * Size of each frame.
     */
    int MINIBASE_BUFFER_POOL_SIZE = 1024;   // in Frames

    int MAX_SPACE = 1024;   // in Frames

    /**
     * in Pages => the DBMS Manager tells the DB how much disk
     * space is available for the database.
     */
    int MINIBASE_DB_SIZE = 10000;
    int MINIBASE_MAX_TRANSACTIONS = 100;
    int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;

    /**
     * also the name of a relation
     */
    int MAXFILENAME = 15;
    int MAXINDEXNAME = 40;
    int MAXATTRNAME = 15;
    int MAX_NAME = 50;

    int INVALID_PAGE = -1;

    String DELIMITER = ",";
    // Used to include the high filter also while fetching from BTree Index
    String rangeRegex = "\\[\\S+,\\S+\\]";
    // detect square bracket
    String squareBracketRegex = "[\\[ \\]]";
    // filter all query
    String filterAllQuery = "*";
    // last character to append for hi_index
    String lastChar = "Z";
}
