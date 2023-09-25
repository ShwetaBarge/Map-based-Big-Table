package starter;

import bufmgr.*;
import global.AttrType;
import global.SystemDefs;

import java.io.*;


public class BigTable {
    public static final AttrType[] BIGT_ATTR_TYPES = new AttrType[]{
            new AttrType(0),
            new AttrType(0),
            new AttrType(1),
            new AttrType(0)
    };
    public static short[] BIGT_STR_SIZES = new short[]{
            (short) 25,  //row
            (short) 25,  //col
            (short) 25  // val
    };
    public static int orderType = 1;
    public static final int NUM_PAGES = 100000;

    public static void main(String[] args) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {

        String input;
        String queryInput;
        String[] queryInputStr = null;

        while (true) {
            System.out.print("BigTable>  ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            input = br.readLine();
            queryInputStr = input.trim().split("\\s+");
            long startTime = System.currentTimeMillis();

            try {
                if (queryInputStr[0].equalsIgnoreCase("exit"))
                    break;
                else if (queryInputStr[0].equalsIgnoreCase("batchinsert")) {
                    // batchinsert DATAFILENAME TYPE BIGTABLENAME
                    // batchinsert /Users/kaushal/Desktop/test_data1.csv 1 testdb1
                    // batchinsert /Users/kaushal/Desktop/phase-1/test_data2.csv 1 testdb9

//                    System.out.println("Format: batchinsert DATAFILENAME TYPE BIGTABLENAME");
//                    System.out.print("command: ");

                    // reset the time
                    boolean exists = false;
                    startTime = System.currentTimeMillis();
                    String dataFile = queryInputStr[1];
                    BIGT_STR_SIZES = setBigTConstants(dataFile);
                    Integer type = Integer.parseInt(queryInputStr[2]);
                    String tableName = queryInputStr[3];

                    if( checkDBExists(tableName)) {
                        exists = true;
                    }else {
                        exists = false;
                    }
                    // Set the metadata name for the given DB. This is used to set the headers for the Maps
                    File file = new File("G:/College-data/" + tableName + "_metadata.txt");
                    FileWriter fileWriter = new FileWriter(file);
                    BufferedWriter bufferedWriter =
                            new BufferedWriter(fileWriter);
                    bufferedWriter.write(dataFile);
                    bufferedWriter.close();
                    Commands.batchInsert(dataFile, tableName, type, exists);
                }
                // Query command
                else if (queryInputStr[0].equalsIgnoreCase("query")) {

                    //query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
                    // query testdb1 1 1 [Greece,Morocco] * * 500
                    // query testdb1 1 1 [Greece,Morocco] [Camel,Moose] * 500

//                    System.out.println("Format: query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
//                    System.out.print("command: ");
//                    queryInput = br.readLine();

                    // reset the time
                    startTime = System.currentTimeMillis();


                    String tableName = queryInputStr[1].trim();
                    String filename = "G:/College-data/" + tableName + "_metadata.txt";

                    FileReader fileReader;
                    BufferedReader bufferedReader;
                    try {
                        fileReader = new FileReader(filename);
                        bufferedReader = new BufferedReader(fileReader);
                    }
                    catch (FileNotFoundException e){
                        System.out.println("Given table name does not exist\n\n");
                        continue;
                    }
                    String metadataFile = bufferedReader.readLine();
                    bufferedReader.close();
                    BIGT_STR_SIZES = setBigTConstants(metadataFile);
                    Integer type = Integer.parseInt(queryInputStr[2]);
                    orderType = Integer.parseInt(queryInputStr[3]);
                    String rowFilter = queryInputStr[4].trim();
                    String colFilter = queryInputStr[5].trim();
                    String valFilter = queryInputStr[6].trim();
                    Integer NUMBUF = Integer.parseInt(queryInputStr[7]);
                    checkDBMissing(tableName);
                    Commands.query(tableName, type, orderType, rowFilter, colFilter, valFilter, NUMBUF);
                } else {
                    System.out.println("Invalid input, enter b/w 1 and 3.\n\n");
                    continue;
                }
            } catch (Exception e) {
                System.out.println("Invalid parameters. Try again.\n\n");
                e.printStackTrace();
                continue;
            }
            SystemDefs.JavabaseBM.flushAllPages();

            final long endTime = System.currentTimeMillis();
            System.out.println("Total time taken: " + (endTime - startTime) / 1000.0 + " seconds");
        }

        System.out.print("quiting...");
    }

    private static short[] setBigTConstants(String dataFileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(dataFileName))) {
            String line;
            int maxRowKeyLength = Short.MIN_VALUE;
            int maxColumnKeyLength = Short.MIN_VALUE;
            int maxValueLength = Short.MIN_VALUE;
            int maxTimeStampLength = Short.MIN_VALUE;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                OutputStream out = new ByteArrayOutputStream();
                DataOutputStream rowStream = new DataOutputStream(out);
                DataOutputStream columnStream = new DataOutputStream(out);
                DataOutputStream timestampStream = new DataOutputStream(out);
                DataOutputStream valueStream = new DataOutputStream(out);

                rowStream.writeUTF(fields[0]);
                maxRowKeyLength = Math.max(rowStream.size(), maxRowKeyLength);

                columnStream.writeUTF(fields[1]);
                maxColumnKeyLength = Math.max(columnStream.size(), maxColumnKeyLength);

                timestampStream.writeUTF(fields[2]);
                maxTimeStampLength = Math.max(timestampStream.size(), maxTimeStampLength);

                valueStream.writeUTF(fields[3]);
                maxValueLength = Math.max(valueStream.size(), maxValueLength);

            }
            return new short[]{
                    (short) maxRowKeyLength,
                    (short) maxColumnKeyLength,
                    (short) maxValueLength
            };
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return new short[0];
    }

    private static boolean checkDBExists(String dbName) {
        String dbPath = getDBPath(dbName);
        File f = new File(dbPath);
//        if (f.exists()) {
//            System.out.println("DB already exists. Exiting.");
//            System.exit(0);
//        }
        return f.exists();
    }

    private static void checkDBMissing(String dbName) {
        String dbPath = getDBPath(dbName);
        File f = new File(dbPath);
        if (!f.exists()) {
            System.out.println("DB does not exist. Exiting.");
            System.exit(0);
        }
//        return f.exists();
    }

    public static String getDBPath(String tableName) {
        return "G:/College-data/" + tableName  + ".db";
    }
}
