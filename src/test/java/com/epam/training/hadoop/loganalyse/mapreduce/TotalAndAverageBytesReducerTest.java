package com.epam.training.hadoop.loganalyse.mapreduce;

import com.epam.training.hadoop.loganalyse.map.TotalBytesAndCountWritable;
import com.epam.training.hadoop.loganalyse.reduce.TotalAndAverageBytesWritable;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.epam.training.hadoop.loganalyse.reduce.TotalAndAverageBytesReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/23/2016
*/

public class TotalAndAverageBytesReducerTest {

    private static final Reducer REDUCER = new TotalAndAverageBytesReducer();
    private static ReduceDriver<Text, TotalBytesAndCountWritable, Text, TotalAndAverageBytesWritable> reduceDriver;

    public TotalAndAverageBytesReducerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        reduceDriver = ReduceDriver.newReduceDriver(REDUCER);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testMyCombiner() {
        List<Pair<Text, TotalAndAverageBytesWritable>> expectedOutput = new LinkedList<>();
        expectedOutput.add(new Pair<>(new Text("ip1"), new TotalAndAverageBytesWritable(96956L, 48478L)));
        expectedOutput.add(new Pair<>(new Text("ip2"), new TotalAndAverageBytesWritable(318L, 318L)));
        expectedOutput.add(new Pair<>(new Text("ip3"), new TotalAndAverageBytesWritable(72209L, 72209L)));

        try {
            reduceDriver
                    .withInput(new Pair<>(new Text("ip1"), Arrays.asList(new TotalBytesAndCountWritable(1L, 40028L), new TotalBytesAndCountWritable(1L, 56928L))))
                    .withInput(new Pair<>(new Text("ip2"), Arrays.asList(new TotalBytesAndCountWritable(1L, 318L))))
                    .withInput(new Pair<>(new Text("ip3"), Arrays.asList(new TotalBytesAndCountWritable(1L, 72209L))))
                    .withAllOutput(expectedOutput)
                    .runTest();
        } catch (IOException ex) {
            fail("IOException should not be thrown");
        }

    }
}
