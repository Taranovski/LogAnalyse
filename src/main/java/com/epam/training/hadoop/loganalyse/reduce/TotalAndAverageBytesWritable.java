package com.epam.training.hadoop.loganalyse.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class TotalAndAverageBytesWritable implements Writable {

    private Long totalBytes;
    private Long averageBytes;

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeLong(totalBytes);
        d.writeLong(averageBytes);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        totalBytes = di.readLong();
        averageBytes = di.readLong();
    }

    public TotalAndAverageBytesWritable() {
    }

    public TotalAndAverageBytesWritable(Long totalBytes, Long averageBytes) {
        this.totalBytes = totalBytes;
        this.averageBytes = averageBytes;
    }

    public Long getAverageBytes() {
        return averageBytes;
    }

    public void setAverageBytes(Long averageBytes) {
        this.averageBytes = averageBytes;
    }

    public Long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(Long totalBytes) {
        this.totalBytes = totalBytes;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 17 * hash + Objects.hashCode(this.totalBytes);
        hash = 17 * hash + Objects.hashCode(this.averageBytes);
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
        final TotalAndAverageBytesWritable other = (TotalAndAverageBytesWritable) obj;
        if (!Objects.equals(this.totalBytes, other.totalBytes)) {
            return false;
        }
        if (!Objects.equals(this.averageBytes, other.averageBytes)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TotalAndAverageBytesWritable{" + "totalBytes=" + totalBytes + ", averageBytes=" + averageBytes + '}';
    }

}
