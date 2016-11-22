package com.epam.training.hadoop.loganalyse.domain;

import org.joda.time.DateTime;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class LogRecord {

    private String ip;
    private String someUnknownFields1;
    private DateTime dateTime;
    private String methodName;
    private String localLink;
    private String protocolName;
    private Integer status;
    private Long bytesTransfered;
    private String hostLink;
    private String miscInfo;

    public LogRecord() {
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getSomeUnknownFields1() {
        return someUnknownFields1;
    }

    public void setSomeUnknownFields1(String someUnknownFields1) {
        this.someUnknownFields1 = someUnknownFields1;
    }

    public DateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(DateTime dateTime) {
        this.dateTime = dateTime;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getLocalLink() {
        return localLink;
    }

    public void setLocalLink(String localLink) {
        this.localLink = localLink;
    }

    public String getProtocolName() {
        return protocolName;
    }

    public void setProtocolName(String protocolName) {
        this.protocolName = protocolName;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getBytesTransfered() {
        return bytesTransfered;
    }

    public void setBytesTransfered(Long bytesTransfered) {
        this.bytesTransfered = bytesTransfered;
    }

    public String getHostLink() {
        return hostLink;
    }

    public void setHostLink(String hostLink) {
        this.hostLink = hostLink;
    }

    public String getMiscInfo() {
        return miscInfo;
    }

    public void setMiscInfo(String miscInfo) {
        this.miscInfo = miscInfo;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (this.ip != null ? this.ip.hashCode() : 0);
        hash = 59 * hash + (this.someUnknownFields1 != null ? this.someUnknownFields1.hashCode() : 0);
        hash = 59 * hash + (this.dateTime != null ? this.dateTime.hashCode() : 0);
        hash = 59 * hash + (this.methodName != null ? this.methodName.hashCode() : 0);
        hash = 59 * hash + (this.localLink != null ? this.localLink.hashCode() : 0);
        hash = 59 * hash + (this.protocolName != null ? this.protocolName.hashCode() : 0);
        hash = 59 * hash + (this.status != null ? this.status.hashCode() : 0);
        hash = 59 * hash + (this.bytesTransfered != null ? this.bytesTransfered.hashCode() : 0);
        hash = 59 * hash + (this.hostLink != null ? this.hostLink.hashCode() : 0);
        hash = 59 * hash + (this.miscInfo != null ? this.miscInfo.hashCode() : 0);
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
        final LogRecord other = (LogRecord) obj;
        if ((this.ip == null) ? (other.ip != null) : !this.ip.equals(other.ip)) {
            return false;
        }
        if ((this.someUnknownFields1 == null) ? (other.someUnknownFields1 != null) : !this.someUnknownFields1.equals(other.someUnknownFields1)) {
            return false;
        }
        if (this.dateTime != other.dateTime && (this.dateTime == null || !this.dateTime.equals(other.dateTime))) {
            return false;
        }
        if ((this.methodName == null) ? (other.methodName != null) : !this.methodName.equals(other.methodName)) {
            return false;
        }
        if ((this.localLink == null) ? (other.localLink != null) : !this.localLink.equals(other.localLink)) {
            return false;
        }
        if ((this.protocolName == null) ? (other.protocolName != null) : !this.protocolName.equals(other.protocolName)) {
            return false;
        }
        if (this.status != other.status && (this.status == null || !this.status.equals(other.status))) {
            return false;
        }
        if (this.bytesTransfered != other.bytesTransfered && (this.bytesTransfered == null || !this.bytesTransfered.equals(other.bytesTransfered))) {
            return false;
        }
        if ((this.hostLink == null) ? (other.hostLink != null) : !this.hostLink.equals(other.hostLink)) {
            return false;
        }
        if ((this.miscInfo == null) ? (other.miscInfo != null) : !this.miscInfo.equals(other.miscInfo)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "LogRecord{" + "ip=" + ip + ", someUnknownFields1=" + someUnknownFields1 + ", dateTime=" + dateTime + ", methodName=" + methodName + ", localLink=" + localLink + ", protocolName=" + protocolName + ", status=" + status + ", bytesTransfered=" + bytesTransfered + ", hostLink=" + hostLink + ", miscInfo=" + miscInfo + '}';
    }

}
