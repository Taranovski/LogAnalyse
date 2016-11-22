package com.epam.training.hadoop.loganalyse.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class TotalBytesAndCountWritable implements Writable {

    private Long times;
    private Long bytes;

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeLong(times);
        d.writeLong(bytes);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        times = di.readLong();
        bytes = di.readLong();
    }

    public TotalBytesAndCountWritable(Long times, Long bytes) {
        this.times = times;
        this.bytes = bytes;
    }

    public TotalBytesAndCountWritable() {
    }

    public Long getTimes() {
        return times;
    }

    public void setTimes(Long times) {
        this.times = times;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + (this.times != null ? this.times.hashCode() : 0);
        hash = 71 * hash + (this.bytes != null ? this.bytes.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TotalBytesAndCountWritable other = (TotalBytesAndCountWritable) obj;
        if (this.times != other.times && (this.times == null || !this.times.equals(other.times))) {
            return false;
        }
        if (this.bytes != other.bytes && (this.bytes == null || !this.bytes.equals(other.bytes))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TotalBytesAndCountWritable{" + "times=" + times + ", bytes=" + bytes + '}';
    }

}
