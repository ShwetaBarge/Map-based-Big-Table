package BigT;

import btree.*;
import bufmgr.*;
import starter.BigTable;
import global.*;
import heap.*;
import iterator.MapUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class bigT {
    // Indexing type, ie the type of index created
    //
    int indexType;
    // Name of the BigT file
    String fileName;
    // Actual btree index file containing the index
    BTreeFile indexFile;
    // Btree Index file on timestamp when index type is 3
    BTreeFile timestampIndexFile;
    // Heap file which stores maps
    Heapfile heapfile;
    // HashMap used for maintaining different (3) map versions
    java.util.Map<String, List<MID>> mapVersion;

    String splitRegex = ",";

    /**
     * Constructor initializes the big table
     *
     * @param fileName - Name of the database file
     * @param indexType - Type of indexing
     */
    public bigT(String fileName, int indexType) {
        try {
            this.fileName = fileName;
            this.indexType = indexType;

            // Create a new .meta heap file for storing the metadata of the table
            Heapfile metadataFile = new Heapfile(fileName + ".meta");
            Tuple metadata = new Tuple();
            metadata.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            metadata.setIntFld(1, this.indexType);
            metadataFile.insertRecord(metadata.getTupleByteArray());

            // heap file for storing the Maps
            this.heapfile = new Heapfile(fileName + ".heap");

            // Initialize the HashMap used for maintaining versions
            this.mapVersion = new HashMap<>();

            // create and initialize the index files
            createIndex();
        } catch (Exception e) {
            System.out.println("From bigT(filename, type)");
            e.printStackTrace();
        }
    }

    /**
     * Constructor for opening existing bigt file used for query command
     * @param fileName
     */
    public bigT(String fileName) {
        this.fileName = fileName;
        try {
            // if metadata file not found on disk
            if (SystemDefs.JavabaseDB.get_file_entry(fileName + ".meta") == null) {
                throw new java.io.FileNotFoundException("bigT metadata " + fileName + " not found");
            }
            
            // Load the metadata from .meta heapfile
            Heapfile metadataFile = new Heapfile(fileName + ".meta");
            Scan metadataFileScan = metadataFile.openScan();
            Tuple metadata = metadataFileScan.getNext(new RID());
            metadata.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            metadataFileScan.closescan();
            this.indexType = metadata.getIntFld(1);
            
            // Set the Index file names from the type
            loadIndex();
            
            // Open the Heap file which is used for storing the maps
            this.heapfile = new Heapfile(fileName + ".heap");
            
            // Load the mapVersion HashMap from the disk
            try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("G:/College-data/" + this.fileName + ".hashmap.ser"))) {
                this.indexType = objectInputStream.readByte();
                this.mapVersion = (HashMap<String, List<MID>>) objectInputStream.readObject();
            } catch (IOException e) {
                throw new IOException("File not writable: " + e.toString());
            }
        } catch (Exception e) {
            System.out.println("From bigT(filename)");
            e.printStackTrace();
        }
    }

    /**
     *
     * @return index type of the database
     */
    public int getIndexType() {
        return this.indexType;
    }

    /**
     *
     * @return number of maps in the bigtable
     * @throws HFBufMgrException
     * @throws IOException
     * @throws HFDiskMgrException
     * @throws InvalidSlotNumberException
     * @throws InvalidTupleSizeException
     */
    public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        return this.heapfile.getRecCnt();
    }

    /**
     *
     * @return number of distinct row labels in the bigtable
     */
    public int getRowCnt() {
        Set<String> rowSet = new HashSet<>();
        mapVersion.keySet().forEach(row -> rowSet.add(row.split(splitRegex)[0]));
        return rowSet.size();
    }

    /**
     *
     * @return number of distinct column labels in the bigtable
     */
    public int getColumnCnt() {
        Set<String> colSet = new HashSet<>();
        mapVersion.keySet().forEach(row -> colSet.add(row.split(splitRegex)[1]));
        return colSet.size();
    }

    /**
     * Inserts map into the big table and return its Mid.
     * At any given time at most 3 versions of map with same name and column but different timestamp are maintained
     * @param mapPtr
     * @return mid
     * @throws Exception
     */
    public MID insertMap(byte[] mapPtr) throws Exception {
        // map to be inserted
        Map map = new Map();
        map.setData(mapPtr);

        String key;
        String mapVersionKey = map.getRowLabel() + GlobalConst.DELIMITER + map.getColumnLabel();
        // list of MIDs of map versions that are stored currently
        List<MID> mapVersionlist = mapVersion.get(mapVersionKey);
        if (mapVersionlist == null) {
            mapVersionlist = new ArrayList<>();
        } else {
            int oldestTimestamp = Integer.MAX_VALUE;
            MID oldestMID = null;
            Map oldestMap = new Map();
            // there can't be more than 3 map versions
            if (mapVersionlist.size() > 3) {
                throw new IOException("database file is corrupted.");
            }
            // if the number of map versions are 3 then we have to remove the oldest timestamp one
            if (mapVersionlist.size() == 3) {
                // loop through the maps that are stored currently and find the oldest timestamp among them
                for (MID currMapMid : mapVersionlist) {
                    Map currMap = heapfile.getMap(currMapMid);
                    if (MapUtils.Equal(currMap, map)) {
                        return currMapMid;
                    } else {
                        if (currMap.getTimeStamp() < oldestTimestamp) {
                            oldestTimestamp = currMap.getTimeStamp();
                            oldestMID = currMapMid;
                            oldestMap = currMap;
                        }
                    }
                }
            }
            // if the map to be inserted is older than the oldest map versions than we skip insertion
            if (mapVersionlist.size() == 3 && map.getTimeStamp() < oldestTimestamp) {
                return oldestMID;
            }

            // if we have 3 versions already we have to remove the oldest one to make room for new map
            if (mapVersionlist.size() == 3) {
                // remove the oldest map from index files
                switch (this.indexType) {
                    case 1:
                        key = oldestMap.getRowLabel();
                        break;
                    case 2:
                        key = oldestMap.getColumnLabel();
                        break;
                    case 3:
                        key = oldestMap.getColumnLabel() + GlobalConst.DELIMITER + oldestMap.getRowLabel();
                        this.timestampIndexFile.Delete(new IntegerKey(oldestMap.getTimeStamp()), MapUtils.copy_mid_to_rid(oldestMID));
                        break;
                    case 4:
                        key = oldestMap.getRowLabel() + GlobalConst.DELIMITER + oldestMap.getValue();
//                        this.timestampIndexFile.Delete(new IntegerKey(oldestMap.getTimeStamp()), MapUtils.copy_mid_to_rid(oldestMID));
                        break;
                    case 5:
                        key = oldestMap.getValue();
                        break;
                    case 6:
                        key = oldestMap.getColumnLabel() + GlobalConst.DELIMITER + oldestMap.getRowLabel();
                        break;
                    case 7:
                        key = null;
                        break;
                    default:
                        throw new Exception("Invalid Index Type");
                }
                if (key != null) {
                    this.indexFile.Delete(new StringKey(key), MapUtils.copy_mid_to_rid(oldestMID));
                }
                // remove the oldest map from heap
                heapfile.deleteMap(oldestMID);
                mapVersionlist.remove(oldestMID);
            }
        }

        // create new map
        MID mid = this.heapfile.insertMap(mapPtr);
        RID rid = MapUtils.copy_mid_to_rid(mid);
        mapVersionlist.add(mid);
        mapVersion.put(mapVersionKey, mapVersionlist);

        // insert new map into index files
        switch (this.indexType) {
            case 1:
                key = map.getRowLabel();
                break;
            case 2:
                key = map.getColumnLabel();
                break;
            case 3:
                key = map.getColumnLabel() + GlobalConst.DELIMITER + map.getRowLabel();
                this.timestampIndexFile.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
            case 4:
                key = map.getRowLabel() + GlobalConst.DELIMITER + map.getValue();
//                this.timestampIndexFile.insert(new IntegerKey(map.getTimeStamp()), rid);
                break;
            case 5:
                key = map.getValue();
                break;
            case 6:
                key = map.getColumnLabel() + GlobalConst.DELIMITER + map.getRowLabel();
                break;
            case 7:
                key = null;
                break;
            default:
                throw new Exception("Invalid Index Type");
        }
        if (key != null) {
            this.indexFile.insert(new StringKey(key), rid);
        }
        return mid;
    }


    /**
     *
     * Opens and returns a stream of map for querying
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @return new stream instance
     * @throws Exception
     */
    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }

    /**
     * Closes the bigtable file
     * @throws PageUnpinnedException
     * @throws PageNotFoundException
     * @throws IOException
     * @throws HashEntryNotFoundException
     * @throws InvalidFrameNumberException
     * @throws ReplacerException
     */
    public void close() throws PageUnpinnedException, PageNotFoundException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
        if (this.indexFile != null) this.indexFile.close();
        if (this.timestampIndexFile != null) this.timestampIndexFile.close();
        
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("G:/College-data/" + this.fileName + ".hashmap.ser"))) {
            objectOutputStream.writeByte(indexType);
            objectOutputStream.writeObject(mapVersion);
        } catch (IOException e) {
            System.out.println("Exception from bigT.close");
            throw new IOException("Error while writing to file: " + e);
        }
    }

    /**
     * Creates btree index files and returns the instance of the files in variable
     * @throws Exception
     */
    private void createIndex() throws Exception {
        switch (this.indexType) {
            case 1:
                // index on row
                this.indexFile = new BTreeFile(
                                    this.fileName + "_row.idx",
                                    AttrType.attrString,
                                    BigTable.BIGT_STR_SIZES[0],
                                    DeleteFashion.NAIVE_DELETE
                                );
                break;
            case 2:
                // index on column
                this.indexFile = new BTreeFile(
                                    this.fileName + "_col.idx",
                                    AttrType.attrString,
                                    BigTable.BIGT_STR_SIZES[1],
                                    DeleteFashion.NAIVE_DELETE
                                );
                break;
            case 3:
                // two indices on column + row and timestamp
                this.indexFile = new BTreeFile(
                                    this.fileName + "_col_row.idx",
                                    AttrType.attrString,
                                    BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[1] + GlobalConst.DELIMITER.getBytes().length,
                                    DeleteFashion.NAIVE_DELETE
                                );
                this.timestampIndexFile = new BTreeFile(
                                            this.fileName + "_timestamp.idx",
                                            AttrType.attrInteger,
                                            4,
                                            DeleteFashion.NAIVE_DELETE
                                        );
                break;
            case 4:
                // two indices on row + value
                this.indexFile = new BTreeFile(
                                    this.fileName + "_row_val.idx",
                                    AttrType.attrString,
                                    BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[2] + GlobalConst.DELIMITER.getBytes().length,
                                    DeleteFashion.NAIVE_DELETE
                                );
                break;
            case 5:
                // index on value
                this.indexFile = new BTreeFile(
                                    this.fileName + "_val.idx",
                                    AttrType.attrString,
                                    BigTable.BIGT_STR_SIZES[2],
                                    DeleteFashion.NAIVE_DELETE
                            );
                break;
            case 6:
                // index on row and column
                this.indexFile = new BTreeFile(
                                this.fileName + "_row_col.idx",
                                AttrType.attrString,
                                BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[1] + GlobalConst.DELIMITER.getBytes().length,
                                DeleteFashion.NAIVE_DELETE
                        );
                break;
            case 7:
                this.indexFile = null;
                break;
            default:
                throw new Exception("Index Type is not valid, enter a index type between 1 and 4");
        }
    }

    /**
     * Opens and loads the btree files into variables
     * @throws Exception
     */
    private void loadIndex() throws Exception {
        switch (this.indexType) {
            case 1:
                this.indexFile = new BTreeFile(this.fileName + "_row.idx");
                break;
            case 2:
                this.indexFile = new BTreeFile(this.fileName + "_col.idx");
                break;
            case 3:
                this.indexFile = new BTreeFile(this.fileName + "_col_row.idx");
                this.timestampIndexFile = new BTreeFile(this.fileName + "_timestamp.idx");
                break;
            case 4:
                this.indexFile = new BTreeFile(this.fileName + "_row_val.idx");
                // this.timestampIndexFile = new BTreeFile(this.fileName + "_timestamp.idx");
                break;
            case 5:
                this.indexFile = new BTreeFile(this.fileName + "_val.idx");
                break;
            case 6:
                this.indexFile = new BTreeFile(this.fileName + "_row_col.idx");
                break;
            case 7:
                this.indexFile = null;
                break;
            default:
                throw new Exception("Index Type is not valid, enter a index type between 1 and 5");
        }
    }

/**
 * row
 * col
 * row+col, time
 * row+val
 * val
 * row+col
 *
 */
}