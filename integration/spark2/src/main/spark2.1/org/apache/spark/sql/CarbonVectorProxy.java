package org.apache.spark.sql;

import java.math.BigInteger;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class CarbonVectorProxy_old {

    private ColumnVector columnVector;
    private ColumnarBatch columnarBatch;

    public CarbonVectorProxy_old(MemoryMode memMode, StructType outputSchema, int rowNum) {
        columnarBatch = ColumnarBatch.allocate( outputSchema, memMode);
    }

    public int capacity() {
        return columnarBatch.capacity();
    }

    public void reset() {
         columnVector.reset();
    }


    public void putRowToColumnBatch(ColumnVector colVector, int rowId, Object value) {
        this.columnVector = columnVector;
        org.apache.spark.sql.types.DataType t = columnVector.dataType();
        if (null == value) {
        	columnVector.putNull(rowId);
        } else {
            if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
            	columnVector.putBoolean(rowId, (boolean) value);
            } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
            	columnVector.putByte(rowId, (byte) value);
            } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
            	columnVector.putShort(rowId, (short) value);
            } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
            	columnVector.putInt(rowId, (int) value);
            } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
            	columnVector.putLong(rowId, (long) value);
            } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
            	columnVector.putFloat(rowId, (float) value);
            } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
            	columnVector.putDouble(rowId, (double) value);
            } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
                UTF8String v = (UTF8String) value;
                columnVector.putByteArray(rowId, v.getBytes());
            } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
                DecimalType dt = (DecimalType) t;
                Decimal d = Decimal.fromDecimal(value);
                if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
                	columnVector.putInt(rowId, (int) d.toUnscaledLong());
                } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
                	columnVector.putLong(rowId, d.toUnscaledLong());
                } else {
                    final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
                    byte[] bytes = integer.toByteArray();
                    columnVector.putByteArray(rowId, bytes, 0, bytes.length);
                }
            } else if (t instanceof CalendarIntervalType) {
                CalendarInterval c = (CalendarInterval) value;
                columnVector.getChildColumn(0).putInt(rowId, c.months);
                columnVector.getChildColumn(1).putLong(rowId, c.microseconds);
            } else if (t instanceof org.apache.spark.sql.types.DateType) {
            	columnVector.putInt(rowId, (int) value);
            } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
            	columnVector.putLong(rowId, (long) value);
            }
        }

    }

    public void putBoolean(int rowId, boolean value) {

        columnVector.putBoolean(rowId, (boolean) value);
    }

    public void putByte(int rowId, byte value) {

        columnVector.putByte(rowId, (byte) value);
    }

    public void putShort(int rowId, short value) {

        columnVector.putShort(rowId, (short) value);
    }

    public void putInt(int rowId, int value) {

        columnVector.putInt(rowId, (int) value);
    }

    public void putFloat(int rowId, float value) {

        columnVector.putFloat(rowId, (float) value);
    }

    public void putLong(int rowId, long value) {

        columnVector.putLong(rowId, (long) value);
    }

    public void putDouble(int rowId, double value) {

        columnVector.putDouble(rowId, (double) value);
    }

    public void putByteArray(int rowId, byte[] value) {

        columnVector.putByteArray(rowId, (byte[]) value);
    }

    public void putInts(int rowId, int count, int value) {

        columnVector.putInts(rowId, count, value);
    }

    public void putShorts(int rowId, int count, short value) {

        columnVector.putShorts(rowId, count, value);
    }

    public void putLongs(int rowId, int count, long value) {

        columnVector.putLongs(rowId, count, value);
    }

    public void putDecimal(int rowId, Decimal value, int precision) {
        columnVector.putDecimal(rowId, value, precision);

    }

    public void putDoubles(int rowId, int count, double value) {

        columnVector.putDoubles(rowId, count, value);
    }

    public void putByteArray(int rowId, byte[] value, int offset, int length) {

        columnVector.putByteArray(rowId, (byte[]) value, offset, length);
    }

    public void putNull(int rowId) {

        columnVector.putNull(rowId);
    }

    public void putNulls(int rowId, int count) {

        columnVector.putNulls(rowId, count);
    }

    public boolean isNullAt(int rowId) {

        return columnVector.isNullAt(rowId);
    }

    public DataType dataType() {

        return columnVector.dataType();
    }

}
