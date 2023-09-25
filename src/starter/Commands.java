package starter;

import BigT.Map;
import BigT.Stream;
import BigT.bigT;
import bufmgr.*;
import diskmgr.pcounter;
import global.BOMSkipper;
import global.MID;
import global.SystemDefs;

import java.io.*;

import static global.GlobalConst.NUMBUF;

public class Commands {

    static void batchInsert(String dataFile, String tableName, int type, boolean exists) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException {
        String dbPath = BigTable.getDBPath(tableName);
//        System.out.println(dbPath);
        File f = new File(dbPath);
        new SystemDefs(dbPath, BigTable.NUM_PAGES, NUMBUF, "Clock");
        pcounter.initialize();

        FileInputStream fileStream;
        BufferedReader br;
        try {
            bigT bigTable;
            if (exists) {
                bigTable = new bigT(tableName);
            }
            else {
                bigTable = new bigT(tableName, type);
            }

            fileStream = new FileInputStream(dataFile);
            br = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));
            String inputStr;

            BOMSkipper.skip(br);
            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                 // System.out.println("p " + inputStr);

                //set the map
                Map map = new Map();
                map.setMapHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);
                MID mid = bigTable.insertMap(map.getMapByteArray());
                // System.out.println("Minitable wcounter: " + pcounter.wcounter);
                // System.out.println("Minitable rcounter: " + pcounter.rcounter);
            }
            System.out.println("******-----------************-----------******\n\n");


            System.out.println("The map count is: " + bigTable.getMapCnt());
            System.out.println("The number of distinct rows are  = " + bigTable.getRowCnt());
            System.out.println("The number of distinct columns are= " + bigTable.getColumnCnt());




            System.out.println("******-----------************-----------******\n\n");
            System.out.println("The reads done are : " + pcounter.rcounter);
            System.out.println("The writes done are: " + pcounter.wcounter);
            System.out.println("Number of buffers are: " + NUMBUF);


            System.out.println("\n\n******-----------************-----------******\n\n");

            // close bigtable and flush all pages
            bigTable.close();
            fileStream.close();
            br.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();

            System.out.println("Write Count: " + pcounter.wcounter);
            System.out.println("No of buffers: " + NUMBUF);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static void query(String tableName, Integer type, Integer orderType, String rowFilter, String colFilter, String valFilter, Integer NUMBUF) throws Exception {
        String dbPath = BigTable.getDBPath(tableName);
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();
        int matchedResults = 0;

        try {

            bigT bigTable = new bigT(tableName);
            if (!type.equals(bigTable.getIndexType())) {
                System.out.println("Type Mismatch");
                bigTable.close();
                return;
            }
            Stream mapStream = bigTable.openStream(orderType, rowFilter, colFilter, valFilter);

            while (true) {
                Map mapObj = mapStream.getNext();
                if (mapObj == null)
                    break;
                mapObj.print();
                matchedResults++;
            }
            bigTable.close();
            mapStream.closeStream();

        } catch (Exception e) {
            e.printStackTrace();
        }

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("\n\n******-----------************-----------******\n\n");
        System.out.println("matching records are " + matchedResults);
        System.out.println("Number of reads are : " + pcounter.rcounter);
        System.out.println("Number of writes are: " + pcounter.wcounter);
        System.out.println("\n\n******-----------************-----------******\n\n");
    }
}
