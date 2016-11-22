package com.epam.training.hadoop.loganalyse.parser;

import com.epam.training.hadoop.loganalyse.domain.LogRecord;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public interface RecordParser {
    
    LogRecord parseRecord(String record);
    boolean isValidRecord(String record);
}
