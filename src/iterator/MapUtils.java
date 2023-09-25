package iterator;

import java.io.*;
import BigT.Map;
import starter.BigTable;
import global.*;

/**
 * some useful method when processing Map
 */

public class MapUtils {

    /**
     * This function compares a Map with another Map in respective field, and
     * returns:
     * <p>
     * 0        if the two are equal,
     * 1        if the tuple is greater,
     * -1        if the tuple is smaller,
     *
     * @param map1        first map.
     * @param map2       another map.
     * @param map_fld_no the field numbers in the map to be compared.
     * @return 0        if the two are equal,
     * 1        if the tuple is greater,
     * -1        if the tuple is smaller,
     * @throws InvalidFieldNo      Invalid Field nuber
     * @throws IOException         I/O Error
     */
    
    public static int CompareMapWithMap(Map map1, Map map2, int map_fld_no)
            throws IOException,
            InvalidFieldNo {
        int map1_int_fld;
        int map2_int_fld;
        String map1_str_fld;
        String map2_Str_fld;

        switch (map_fld_no) {
            case 0:
                map1_str_fld = map1.getRowLabel();
                map2_Str_fld = map2.getRowLabel();

                if (map1_str_fld.compareTo(map2_Str_fld) <0) return -1;
                if (map1_str_fld.compareTo(map2_Str_fld) >0) return 1;
                return 0;
            case 1:
                map1_str_fld = map1.getColumnLabel();
                map2_Str_fld = map2.getColumnLabel();
                if (map1_str_fld.compareTo(map2_Str_fld) <0) return -1;
                if (map1_str_fld.compareTo(map2_Str_fld) >0) return 1;
                return 0;
            case 2:
                map1_int_fld = map1.getTimeStamp();
                map2_int_fld = map2.getTimeStamp();
                if (map1_int_fld == map2_int_fld) return 0;
                if (map1_int_fld < map2_int_fld) return -1;
                return 1;
            case 3:
                map1_str_fld = map1.getValue();
                map2_Str_fld = map2.getValue();
                if (map1_str_fld.compareTo(map2_Str_fld) <0) return -1;
                if (map1_str_fld.compareTo(map2_Str_fld) >0) return 1;
                return 0;
            default:
                throw new InvalidFieldNo("Total fields in the map is 4. So Field Number range is (0,3)");
        }
    }


    /**
     * set up a tuple in specified field from a tuple
     *
     * @param map1   the map to be set
     * @param map2   the given map
     * @param map_fld_no  the field number
     * @throws IOException         IO Error
     */

    public static void SetValue(Map map1, Map map2, int map_fld_no)
            throws IOException
    {

        switch (map_fld_no) {
            case 1:
                map1.setRowLabel(map2.getRowLabel());
                break;
            case 2:
                map1.setColumnLabel(map2.getColumnLabel());
                break;
            case 3:
                map1.setTimeStamp(map2.getTimeStamp());
                break;
            case 4:
                map1.setValue(map2.getValue());
                break;
        }
    }

    public static boolean Equal(Map map1, Map map2) throws IOException, InvalidFieldNo, TupleUtilsException {

        for (int i = 0; i <= 3; i++) {
            if (CompareMapWithMap(map1, map2, i) != 0)
                return false;
        }
        return true;
    }

    public static RID copy_mid_to_rid(MID mid) {

        return new RID(mid.getPageNo(), mid.getSlotNo());
    }

    /**
     * set up the Jtuple's attrtype, string size,field number for using project
     *
     * @param Jmap       reference to an actual map  - no memory has been malloced
     * @param res_attrs    attributes type of result map
     * @param in1          array of the attributes of the map (ok)
     * @param len_in1      num of attributes of in1
     * @param t1_str_sizes shows the length of the string fields in S
     * @param proj_list    shows what input fields go where in the output map
     * @param nOutFlds     number of outer relation fields
     * @throws IOException         some I/O fault
     * @throws TupleUtilsException exception from this class
     * @throws InvalidRelation     invalid relation
     */

    public static short[] setup_op_tuple(Map Jmap, AttrType[] res_attrs,
                                         AttrType[] in1, int len_in1,
                                         short[] t1_str_sizes,
                                         FldSpec[] proj_list, int nOutFlds)
            throws
            TupleUtilsException,
            InvalidRelation {
        short[] sizesT1 = new short[len_in1];
        int i;
        int count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);

            else throw new InvalidRelation("Invalid relation -innerRel");
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short[n_strs];
        count = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
        }

        try {
            Jmap.setMapHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "setHdr() failed");
        }
        return res_str_sizes;
    }

    
    public static int CompareMapsOnOrderType(Map mapObj1, Map mapObj2) throws IOException {
        int mapRowCompare = mapObj1.getRowLabel().compareTo(mapObj2.getRowLabel());
        int mapColumnCompare = mapObj1.getColumnLabel().compareTo(mapObj2.getColumnLabel());
        boolean mapTsCompare = (mapObj1.getTimeStamp() >= mapObj2.getTimeStamp());

        if (BigTable.orderType == 2) {
            if (mapColumnCompare > 0) return 1;
            else if (mapColumnCompare < 0) return -1;
            else if (mapRowCompare > 0) return 1;
            else if (mapRowCompare < 0) return -1;
            else if (mapTsCompare) return 1;
            else return -1;
        } else if (BigTable.orderType == 3) {
            if (mapRowCompare > 0) return 1;
            else if (mapRowCompare < 0) return -1;
            else {
                if (mapTsCompare) return 1;
                else return -1;
            }
        } else if (BigTable.orderType == 4) {
            if (mapColumnCompare > 0) return 1;
            else if (mapColumnCompare < 0) return -1;
            else {
                if (mapTsCompare) return 1;
                else return -1;
            }
        } else if (BigTable.orderType == 5) {
            if (mapTsCompare) return 1;
            else return -1;
        }
        if (mapRowCompare > 0) return 1;
        else if (mapRowCompare < 0) return -1;
        else if (mapColumnCompare > 0) return 1;
        else if (mapColumnCompare < 0) return -1;
        else {
            if (mapTsCompare) return 1;
            else return -1;
        }
    }
}