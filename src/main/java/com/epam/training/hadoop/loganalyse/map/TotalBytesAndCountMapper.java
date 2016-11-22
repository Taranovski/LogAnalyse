package com.epam.training.hadoop.loganalyse.map;

import com.epam.training.hadoop.loganalyse.domain.LogRecord;
import com.epam.training.hadoop.loganalyse.counter.Errors;
import com.epam.training.hadoop.loganalyse.counter.Browsers;
import com.epam.training.hadoop.loganalyse.BrowserFinder;
import com.epam.training.hadoop.loganalyse.parser.RecordParser;
import com.epam.training.hadoop.loganalyse.parser.RecordParserImpl;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class TotalBytesAndCountMapper extends Mapper<LongWritable, Text, Text, TotalBytesAndCountWritable> {
    
    private static final RecordParser RECORD_PARSER = new RecordParserImpl();
    private static final Text IP = new Text();
    private static final TotalBytesAndCountWritable INTERMEDIATE_RESULT = new TotalBytesAndCountWritable();
    private static final Long ONE = 1L;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String record = value.toString();
        
        if (RECORD_PARSER.isValidRecord(record)) {
            LogRecord logRecord = RECORD_PARSER.parseRecord(record);
            
            IP.set(logRecord.getIp());
            INTERMEDIATE_RESULT.setTimes(ONE);
            INTERMEDIATE_RESULT.setBytes(logRecord.getBytesTransfered());
            
            for (Browsers b : BrowserFinder.getBrowsers(logRecord.getMiscInfo())) {
                context.getCounter(b).increment(1L);
            }
            
            context.write(IP, INTERMEDIATE_RESULT);
            
        } else {
            context.getCounter(Errors.ERRORS).increment(1);
        }
        
    }
    
}
