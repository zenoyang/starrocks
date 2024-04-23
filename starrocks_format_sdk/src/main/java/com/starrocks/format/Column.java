package com.starrocks.format;

import com.starrocks.proto.TabletSchema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public class Column {

    static public final List<String> SUPPORT_COLUMN_TYPE = Arrays.asList("INT",
            "BOOLEAN",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "LARGEINT",
            "FLOAT",
            "DOUBLE",
            "DATE",
            "DATETIME",
            "DECIMAL32",
            "DECIMAL64",
            "DECIMAL128",
            "CHAR",
            "VARCHAR");

    private long nativeHandler = 0;
    TabletSchema.ColumnPB columnPB;

    Column(long columnHandler, TabletSchema.ColumnPB columnPB) {
        this.nativeHandler = columnHandler;
        this.columnPB = columnPB;
    }

    // read api
    public boolean isNullAt(long rowIdx) {
        return nativeIsNullAt(nativeHandler, rowIdx);
    }

    public boolean getBoolean(long rowIdx) {
        return nativeGetBool(nativeHandler, rowIdx);
    }

    public byte getByte(long rowIdx) {
        return nativeGetByte(nativeHandler, rowIdx);
    }

    public short getShort(long rowIdx) {
        return nativeGetShort(nativeHandler, rowIdx);
    }

    public int getInt(long rowIdx) {
        return nativeGetInt(nativeHandler, rowIdx);
    }

    public long getLong(long rowIdx) {
        return nativeGetLong(nativeHandler, rowIdx);
    }

    public BigInteger getLargeInt(long rowIdx) {
        byte[] bytes = nativeGetLargeInt(nativeHandler, rowIdx);
        return new BigInteger(bytes);
    }

    public BigDecimal getDecimal(long rowIdx) {
        byte[] bytes = nativeGetDecimal(nativeHandler, rowIdx);
        return new BigDecimal(new BigInteger(bytes), columnPB.getFrac());
    }

    public float getFloat(long rowIdx) {
        return nativeGetFloat(nativeHandler, rowIdx);
    }

    public double getDouble(long rowIdx) {
        return nativeGetDouble(nativeHandler, rowIdx);
    }

    public Date getDate(long rowIdx) {
        long timestamp = nativeGetDate(nativeHandler, rowIdx);
        return new Date(timestamp);
    }

    public Date getDate(long rowIdx, TimeZone timeZone) {
        long timestamp = nativeGetDate(nativeHandler, rowIdx);
        return new Date(timestamp - timeZone.getRawOffset());
    }

    public Timestamp getTimestamp(long rowIdx) {
        long timestamp = nativeGetTimestamp(nativeHandler, rowIdx);
        return new Timestamp(timestamp);
    }

    public Timestamp getTimestamp(long rowIdx, TimeZone timeZone) {
        long timestamp = nativeGetTimestamp(nativeHandler, rowIdx);
        return new Timestamp(timestamp - timeZone.getRawOffset());
    }


    public String getString(long rowIdx) {
        return nativeGetString(nativeHandler, rowIdx);
    }

    public native boolean nativeIsNullAt(long nativeHandler, long rowIdx);

    public native boolean nativeGetBool(long nativeHandler, long rowIdx);

    public native byte nativeGetByte(long nativeHandler, long rowIdx);

    public native short nativeGetShort(long nativeHandler, long rowIdx);

    public native int nativeGetInt(long nativeHandler, long rowIdx);

    public native long nativeGetLong(long nativeHandler, long rowIdx);

    public native byte[] nativeGetLargeInt(long nativeHandler, long rowIdx);

    public native float nativeGetFloat(long nativeHandler, long rowIdx);

    public native double nativeGetDouble(long nativeHandler, long rowIdx);

    public native byte[] nativeGetDecimal(long nativeHandler, long rowIdx);

    public native long nativeGetDate(long nativeHandler, long rowIdx);

    public native long nativeGetTimestamp(long nativeHandler, long rowIdx);

    public native String nativeGetString(long nativeHandler, long rowIdx);

    // write api
    public void appendNull() {
        nativeAppendNull(nativeHandler);
    }

    public void appendBool(boolean value) {
        nativeAppendBool(nativeHandler, value);
    }

    public void appendByte(byte value) {
        nativeAppendByte(nativeHandler, value);
    }

    public void appendShort(short value) {
        nativeAppendShort(nativeHandler, value);
    }

    public void appendInt(int value) {
        nativeAppendInt(nativeHandler, value);
    }

    public void appendLong(long value) {
        nativeAppendLong(nativeHandler, value);
    }

    public void appendLargeInt(BigInteger value) {
        nativeAppendLargeInt(nativeHandler, value.toByteArray());
    }

    public void appendFloat(float value) {
        nativeAppendFloat(nativeHandler, value);
    }

    public void appendDouble(double value) {
        nativeAppendDouble(nativeHandler, value);
    }

    public void appendDecimal(BigDecimal value) {
        if (value.precision() - value.scale() > columnPB.getPrecision() - columnPB.getFrac()) {
            throw new IllegalArgumentException("Decimal '" + value
                    + "' is out of the range. The type of " + columnPB.getName() + " is "
                    + columnPB.getType() + "(" + columnPB.getPrecision() + ", " + columnPB.getFrac() + ").");
        }
        if (columnPB.getFrac() != value.scale()) {
            value = value.setScale(columnPB.getFrac(), BigDecimal.ROUND_HALF_EVEN);
        }
        nativeAppendDecimal(nativeHandler, value.unscaledValue().toByteArray());
    }

    public void appendDate(Date value) {
        nativeAppendDate(nativeHandler, value.getTime());
    }

    public void appendTimestamp(Timestamp value) {
        nativeAppendTimestamp(nativeHandler, value.getTime());
    }

    public void appendString(String value) {
        nativeAppendString(nativeHandler, value);
    }


    public native void nativeAppendNull(long nativeHandler);

    public native void nativeAppendBool(long nativeHandler, boolean value);

    public native void nativeAppendByte(long nativeHandler, byte value);

    public native void nativeAppendShort(long nativeHandler, short value);

    public native void nativeAppendInt(long nativeHandler, int value);

    public native void nativeAppendLong(long nativeHandler, long value);

    public native void nativeAppendLargeInt(long nativeHandler, byte[] value);

    public native void nativeAppendFloat(long nativeHandler, float value);

    public native void nativeAppendDouble(long nativeHandler, double value);

    public native void nativeAppendDecimal(long nativeHandler, byte[] value);

    public native void nativeAppendDate(long nativeHandler, long value);

    public native void nativeAppendTimestamp(long nativeHandler, long value);

    public native void nativeAppendString(long nativeHandler, String value);


}
