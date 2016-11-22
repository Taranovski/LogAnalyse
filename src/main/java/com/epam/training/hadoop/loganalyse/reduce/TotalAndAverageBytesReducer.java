package com.epam.training.hadoop.loganalyse.reduce;

import com.epam.training.hadoop.loganalyse.map.TotalBytesAndCountWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class TotalAndAverageBytesReducer extends Reducer<Text, TotalBytesAndCountWritable, Text, TotalAndAverageBytesWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<TotalBytesAndCountWritable> values, Context context) throws IOException, InterruptedException {
        
        long counter = 0;
        long totalBytes = 0;
        long averageBytes = 0;
        
        for (TotalBytesAndCountWritable intermediateWritable : values) {
            counter += intermediateWritable.getTimes();
            totalBytes += intermediateWritable.getBytes();
        }
        
        averageBytes = totalBytes / counter;
        
        context.write(key, new TotalAndAverageBytesWritable(totalBytes, averageBytes));
        
    }
    
}
