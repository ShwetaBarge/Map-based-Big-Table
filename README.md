# Map-based-Big-Table
A big table based implementation over Minibase.

## Implementation

We have adapted the Minibase distribution to accommodate a wide-columnar store akin to BigTable. Minibase is a relational DBMS that houses data as Tuples. We've enhanced Minibase by introducing a new Map structure with 4 fields.

Additionally, we've incorporated versioning, retaining the 3 latest Maps.

In the same vein as the heap.Scan class in Minibase, we introduce BigT.Stream. This initiates a stream of maps which can be filtered and sequenced based on the orderType. The current filtering options include:

- \* : The star filter which retrieves all entries.
- Individual values.
- Range values delineated by brackets.

If orderType is:

- 1 results are sorted first by row label, followed by column label, and then time stamp.
- 2 results are sorted initially by column label, next by row label, and lastly by time stamp.
- 3 results are sequenced by row label first, then time stamp.
- 4 results are sequenced initially by column label, and subsequently by time stamp.
- 5 results are organized based on the time stamp.

We've enhanced the diskmgr package, responsible for the creation and management of our Btree-based index files that structure our data. The current index types supported are:

- Type 1: No indexing applied.
- Type 2: Utilizes a single Btree for row label indexing, resulting in maps that are sorted by row labels in ascending order.
- Type 3: Incorporates a Btree to index column labels, producing maps sorted by column labels in ascending sequence.
- Type 4: Employs two Btrees: one for indexing a combined key of column and row labels and another for timestamps. Maps are subsequently sorted based on the combined key in ascending fashion.
- Type 5: Uses two Btrees: one for indexing a combination of row label and value, and another for timestamps. Maps are organized in an ascending manner based on the combined key.

## Usage

Build the project and then use the following command to enter the CLI.

```
java cmdline/MiniTable.java

```

The batch insert query is used to insert multiple Maps into a bigTable. A csv with Maps (row, column, timestamp, value) is provided to the batch insert command. The command for batch insert:

```
batchinsert PATH_TO_DATAFILE TYPE BIGTABLENAME NUMBUF

```

To insert a single map into the table use the mapinsert command:

```
mapinsert ROW_LABEL COLUMN_LABEL VALUE TIMESTAMP TYPE BIGTABLENAME NUMBUF

```

To query the data you need to pass the order type along with the filters. `NUMBUF` is the number of buffers which will be used during querying. The command for querying:

```
query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF```

```

We have also implemented a simple join operator to understand how it works.

```
RowJoin(int amt of mem, Stream leftStream, java.lang.String RightBigTName, attrString ColumnName)
amt_of_mem - IN PAGES
leftStream - a stream for the left data source
RightBigTName - name of the BigTable at the right side of the join ColumnName - condition to match the column labels

```

The output produces a series of maps that form a BigT. These maps consist of rows that align with the specified conditions. Specifically:

- Two rows are considered a match only if they share an identical column and their most recent values within that column are the same.
- The resulting row label is formed by joining the two original row labels, separated by a “:”.
- The merged row will include all the column labels from both input rows. However, the shared column, which prompted the match, will appear only once in the BigT. This column will showcase only its three most recent values.

The syntax for row join is as follows:

```
rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER NUMBUF

```

The program will interact with the database to perform a 'rowjoin' operation on the two bigtables, resulting in a new type 1 big table named after the provided table name. During the query execution, Minibase will utilize a maximum of NUMBUF buffer pages.

Furthermore, we've introduced an external row sort operator. This operator produces a type 1 BigT where rows are organized in a non-decreasing sequence based on the latest values for the specified column label.
Tht syntax for row sort is as follows:

```
rowsort INBTNAME OUTBTNAME COLUMNNAME NUMBUF

```

The getCounts command will return the numbers of maps, distinct row labels, and distinct column labels.

```
getCounts NUMBUF

```

## New Changes

The bigDB class has been revised. Its constructor no longer requires a 'type' as an input parameter. This means within a single bigDB database, data can be stored following various storage types.

Changes have also been made to the bigT class. The bigt method within this class no longer demands the 'type' as input. As a result, a single bigT table can have maps stored in different storage formats.
