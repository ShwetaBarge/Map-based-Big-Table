package global;

import java.io.*;

public class MID implements Serializable {

    public int slotNo;

    public PageId pageNo = new PageId();

    public int getSlotNo() {
        return slotNo;
    }

    public void setSlotNo(int slotNo) {
        this.slotNo = slotNo;
    }

    public PageId getPageNo() {
        return pageNo;
    }

    public void setPageNo(PageId pageNo) {
        this.pageNo = pageNo;
    }

    public MID(PageId pageNo, int slotNo) {
        this.slotNo = slotNo;
        this.pageNo = pageNo;
    }

    public MID() {
    }

    public void copyMid(MID mid) {
        this.slotNo = mid.slotNo;
        this.pageNo = mid.pageNo;
    }

    public void writeTOByteArray(byte[] array, int offset) throws IOException {
        Convert.setIntValue(slotNo, offset, array);
        Convert.setIntValue(pageNo.pid, offset + 4, array);
    }

    public boolean equals(MID mid) {

        if ((this.pageNo.pid == mid.pageNo.pid) && (this.slotNo == mid.slotNo))
            return true;
        else
            return false;
    }

}