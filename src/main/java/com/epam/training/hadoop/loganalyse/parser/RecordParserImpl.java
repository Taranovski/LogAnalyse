package com.epam.training.hadoop.loganalyse.parser;

import com.epam.training.hadoop.loganalyse.domain.LogRecord;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class RecordParserImpl implements RecordParser {

    private final static Logger logger = Logger.getLogger(RecordParserImpl.class);

    private final static String EMPTY_RECORD = "";
    private final static Pattern VALID_RECORD = Pattern.compile("(ip\\d+)\\s(.+)\\s(\\[(.+)\\s(.+)\\])\\s(\\\"(\\w+)\\s(\\/.+)\\s(.+)\\\")\\s(\\d+)\\s(\\d+)\\s(\\\"(.+)\\\")\\s(\\\"(.+)\\\")");
    private static Matcher matcher;
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss").withLocale(Locale.ENGLISH);

    @Override
    public boolean isValidRecord(String record) {
        return isValid(record);
    }

    private boolean isValid(String record) {
        if (record == null || EMPTY_RECORD.equals(record)) {
            return false;
        }
        matcher = VALID_RECORD.matcher(record);
        if (matcher.find()) {
            try {
                DateTime.parse(matcher.group(4), DATE_TIME_FORMATTER);
            } catch (RuntimeException e) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    @Override
    public LogRecord parseRecord(String record) {
        if (!isValid(record)) {
            logger.error("invalid record: \n" + record);
            return null;
        }
        LogRecord logRecord = new LogRecord();

        logRecord.setIp(matcher.group(1));
        logRecord.setSomeUnknownFields1(matcher.group(2));
        try {
            DateTime dateTime = DateTime.parse(matcher.group(4), DATE_TIME_FORMATTER).withZoneRetainFields(DateTimeZone.forID(matcher.group(5)));
            logRecord.setDateTime(dateTime);
        } catch (RuntimeException e) {
            logger.error("failed to parse datetime of the record: \n" + record, e);
        }
        logRecord.setMethodName(matcher.group(7));
        logRecord.setLocalLink(matcher.group(8));
        logRecord.setProtocolName(matcher.group(9));
        logRecord.setStatus(Integer.valueOf(matcher.group(10)));
        logRecord.setBytesTransfered(Long.valueOf(matcher.group(11)));
        logRecord.setHostLink(matcher.group(13));
        logRecord.setMiscInfo(matcher.group(15));

        return logRecord;
    }
}
