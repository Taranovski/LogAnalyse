package com.epam.training.hadoop.loganalyse;

import com.epam.training.hadoop.loganalyse.map.TotalBytesAndCountWritable;
import com.epam.training.hadoop.loganalyse.combine.TotalBytesAndCountCombiner;
import com.epam.training.hadoop.loganalyse.map.TotalBytesAndCountMapper;
import com.epam.training.hadoop.loganalyse.reduce.TotalAndAverageBytesReducer;
import com.epam.training.hadoop.loganalyse.reduce.TotalAndAverageBytesWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class LogAnaliseRunner extends Configured implements Tool {

    private static final String INPUT_PATH_CONFIG = "mapreduce.loganalyse.inputpath";
    private static final String OUTPUT_PATH_CONFIG = "mapreduce.loganalyse.outputpath";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LogAnaliseRunner(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        String inputPath = getProperty(conf, INPUT_PATH_CONFIG);
        String outputPath = getProperty(conf, OUTPUT_PATH_CONFIG);

        Job job = Job.getInstance(conf, "Log analyse");

        job.setJarByClass(LogAnaliseRunner.class);

        job.setMapperClass(TotalBytesAndCountMapper.class);
        job.setCombinerClass(TotalBytesAndCountCombiner.class);
        job.setReducerClass(TotalAndAverageBytesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TotalBytesAndCountWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TotalAndAverageBytesWritable.class);
        job.setNumReduceTasks(2);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(inputPath));
        SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private String getProperty(Configuration conf, String inputPathConfig) {
        String inputPath = conf.get(inputPathConfig);
        if (inputPath == null) {
            throw new RuntimeException("no " + inputPathConfig +
                    " set, try to set it by adding -D" + inputPathConfig + "=<path>");
        }
        return inputPath;
    }
}
