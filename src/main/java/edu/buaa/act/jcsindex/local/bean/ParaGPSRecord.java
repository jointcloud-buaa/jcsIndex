package edu.buaa.act.jcsindex.local.bean;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by shmin at 2018/7/5 23:36
 **/
public class ParaGPSRecord implements java.io.Externalizable {
    private static final long serialVersionUID = 6554793519996214470L;

    private int gridId;
    private int rowId;

    private String rowKey;
    private float latitude;
    private float longitude;
    private long gpstime;
    private long devicesn;

    public ParaGPSRecord() {
    }

    public ParaGPSRecord(int gridId, int rowId, float latitude, float longitude, long gpstime, long devicesn) {
        super();
        this.gridId = gridId;
        this.rowId = rowId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.gpstime = gpstime;
        this.devicesn = devicesn;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(gridId);
        out.writeInt(rowId);
        out.writeFloat(latitude);
        out.writeFloat(longitude);
        out.writeLong(gpstime);
        out.writeLong(devicesn);
        out.flush();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO Auto-generated method stub
        this.gridId = in.readInt();
        this.rowId = in.readInt();
        this.latitude = in.readFloat();
        this.longitude = in.readFloat();
        this.gpstime = in.readLong();
        this.devicesn = in.readLong();
    }

    @Override
    public String toString() {
        return "{ devicesn=" + devicesn + ", gpstime=" + gpstime + ", longitude=" + longitude + ", latitude=" + latitude
                + " }";
    }

    public int getGridId() {
        return gridId;
    }

    public void setGridId(int gridId) {
        this.gridId = gridId;
    }

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public long getGpstime() {
        return gpstime;
    }

    public void setGpstime(long gpstime) {
        this.gpstime = gpstime;
    }

    public long getDevicesn() {
        return devicesn;
    }

    public void setDevicesn(long devicesn) {
        this.devicesn = devicesn;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }
}
