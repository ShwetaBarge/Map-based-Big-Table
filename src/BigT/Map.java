package BigT;

import java.io.*;
import global.*;
import heap.*;



public class Map implements GlobalConst {

    public byte[] data;
    public int map_offset;
    public int map_length;
    public short fldCnt;
    public short[] fldOffset;
    public static final short row_pos = 1;
    public static final short col_pos = 2;
    public static final short timstp_pos = 3;
    public static final short val_pos = 4;
    public static final short num_fields = 4;
    public static final int max_size = MINIBASE_PAGESIZE;


    /**This is default Map constructor which initialises a new map.*/
    public Map() {
        this.data = new byte[max_size];
        this.map_length = max_size;
        this.map_offset = 0;
    }

    /**
     * @param map_byte_arr This byte array data is used to initialise a new map.
     * @param offset offset of map.
     * @throws IOException I/O errors
     */
    public Map(byte[] map_byte_arr, int offset) throws IOException {
        this.data = map_byte_arr;
        this.map_offset = offset;
        set_field_offset_from_byte_data();
        setFldCnt(Convert.getShortValue(offset, this.data));
    }

    /**
     * @param map_byte_arr This byte array data is used to initialise a new map.
     * @param offset offset of map.
     * @param map_length length of the map.
     * @throws IOException I/O errors
     */
    public Map(byte[] map_byte_arr, int offset, int map_length) throws IOException {
        this.map_offset = offset;
        this.data = map_byte_arr;
        this.map_length = map_length;
        set_field_offset_from_byte_data();
        setFldCnt(Convert.getShortValue(offset, this.data));
    }

    /**
     * @param fromMap Initialse maps given map object.
     */
    public Map(Map fromMap) {
        this.data = fromMap.getMapByteArray();
        this.map_length = fromMap.getMapLength();
        this.map_offset = 0;
        this.fldCnt = fromMap.getFieldCount();
        this.fldOffset = fromMap.copyFieldOffset();
    }

    /**
     * @param size Initialze map based on given size.
     */
    public Map(int size) {
        this.data = new byte[size];
        this.map_offset = 0;
        this.map_length = size;
        this.fldCnt = 4;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) throws IOException {
        this.data = data;
        set_field_offset_from_byte_data();
        setFldCnt(Convert.getShortValue(0, data));
    }

    public int getMapLength() {
        return map_length;
    }

    public short getFieldCount() {
        return fldCnt;
    }

    public void setFldCnt(short fldCnt) {
        this.fldCnt = fldCnt;
    }

    public short[] getFieldOffset() {
        return fldOffset;
    }

    public String getStringField(short fieldNumber) throws IOException, FieldNumberOutOfBoundException {
        if (fieldNumber == 3) {
            throw new FieldNumberOutOfBoundException(null, "MAP: INVALID_FIELD PASSED");
        } else {
            return Convert.getStrValue(this.fldOffset[fieldNumber - 1], this.data, this.fldOffset[fieldNumber] - this.fldOffset[fieldNumber - 1]);
        }
    }

    public short[] copyFieldOffset() {
        short[] newFieldOffset = new short[this.fldCnt + 1];
        System.arraycopy(this.fldOffset, 0, newFieldOffset, 0, this.fldCnt + 1);
        return newFieldOffset;
    }


    public String getRowLabel() throws IOException {
        return Convert.getStrValue(this.fldOffset[row_pos - 1], this.data, this.fldOffset[row_pos] - this.fldOffset[row_pos - 1]);
    }

    public String getColumnLabel() throws IOException {
        return Convert.getStrValue(this.fldOffset[col_pos - 1], this.data, this.fldOffset[col_pos] - this.fldOffset[col_pos - 1]);
    }

    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(this.fldOffset[timstp_pos - 1], this.data);
    }

    public String getValue() throws IOException {
        return Convert.getStrValue(this.fldOffset[val_pos - 1], this.data, this.fldOffset[val_pos] - this.fldOffset[val_pos - 1]);
    }

    public void setRowLabel(String rowLabel) throws IOException {
        Convert.setStrValue(rowLabel, this.fldOffset[row_pos - 1], this.data);
    }

    public void setColumnLabel(String columnLabel) throws IOException {
        Convert.setStrValue(columnLabel, this.fldOffset[col_pos - 1], this.data);
    }

    public void setTimeStamp(int timeStamp) throws IOException {
        Convert.setIntValue(timeStamp, this.fldOffset[timstp_pos - 1], this.data);
    }

    public void setValue(String value) throws IOException {
        Convert.setStrValue(value, this.fldOffset[val_pos - 1], this.data);
    }

    public byte[] getMapByteArray() {
        byte[] map_copy = new byte[this.map_length];
        System.arraycopy(this.data, this.map_offset, map_copy, 0, this.map_length);
        return map_copy;
    }

    /**
     * Print the map with specified rowLabel, columnLabel and timestamp
     * @throws IOException
     */
    public void print() throws IOException {
        String rowLabel = getRowLabel();
        String columnLabel = getColumnLabel();
        int timestamp = getTimeStamp();
        String value = getValue();
        System.out.println("{RowLabel:" + rowLabel + ", ColumnLabel:" + columnLabel + ", TimeStamp:" + timestamp + ", Value:" + value + "}");
    }

    public short size() {
        return ((short) (this.fldOffset[fldCnt] - this.map_offset));
    }

    /**
     * @param fromMap Copy the map object to this map object.
     */
    public void mapCopy(Map fromMap) {
        byte[] tempArray = fromMap.getMapByteArray();
        System.arraycopy(tempArray, 0, data, map_offset, map_length);
    }

    public void mapInit(byte[] amap, int offset) {
        this.data = amap;
        this.map_offset = offset;
    }

    public void mapSet(byte[] fromMap, int offset) {
        System.arraycopy(fromMap, offset, this.data, 0, this.map_length);
        this.map_offset = 0;
    }

    private void set_field_offset_from_byte_data() throws IOException {
        int position = this.map_offset + 2;
        this.fldOffset = new short[num_fields + 1];

        for (int i=0; i <= num_fields; i++){
            this.fldOffset[i] = Convert.getShortValue(position, this.data);
            position += 2;
        }
    }

    /**
     *
     * @param types
     * @param str_len
     * @throws InvalidMapSizeException
     * @throws IOException
     * @throws InvalidTypeException
     * @throws InvalidStringSizeArrayException
     */
    public void setMapHeader(AttrType[] types, short[] str_len) throws IOException, InvalidTypeException, InvalidMapSizeException, InvalidStringSizeArrayException {

        if (str_len.length != 3) {
            throw new InvalidStringSizeArrayException(null, "String sizes array must exactly be 3");
        }
        this.fldCnt = num_fields;
        Convert.setShortValue(num_fields, this.map_offset, this.data);
        this.fldOffset = new short[num_fields + 1];
        int curr_pos = this.map_offset + 2;
        this.fldOffset[0] = (short) ((num_fields + 2) * 2 + this.map_offset);
        Convert.setShortValue(this.fldOffset[0], curr_pos, data);
        curr_pos += 2;

        short cou_str = 0;

        short offset_add;

        for (short i = 0; i < num_fields; i++) {
            switch (types[i].attrType) {
                case AttrType.attrInteger:
                    offset_add = 4;
                    break;
                case AttrType.attrString:
                    offset_add = (short) (str_len[cou_str++] + 2);
                    break;
                default:
                    throw new InvalidTypeException(null, "Invalid type found (not Int or Str)");
            }
            this.fldOffset[i + 1] = (short) (this.fldOffset[i] + offset_add);
            Convert.setShortValue(this.fldOffset[i + 1], curr_pos, data);
            curr_pos += 2;
        }

        this.map_length = this.fldOffset[num_fields] - this.map_offset;

        if (this.map_length > max_size) {
            throw new InvalidMapSizeException(null, "Map length is greater than expected size");
        }

    }

    public String getGenericValue(String fld_type) throws Exception {
        if (fld_type.matches(".*row.*")) {
            return this.getRowLabel();
        } else if (fld_type.matches(".*column.*")) {
            return this.getColumnLabel();
        } else if (fld_type.matches(".*value.*")) {
            return this.getValue();
        } else {
            throw new Exception("Invalid field type passed.");
        }
    }

    public Map setStrFld(int fldNo, String val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setStrValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Field number passed is out of bound");
    }

    public Map setIntFld(int fldNo, int val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setIntValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Field number passed is out of bound");
    }

    public Map setFloFld(int fldNo, float val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setFloValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "Field number passed is out of bound");

    }
}
