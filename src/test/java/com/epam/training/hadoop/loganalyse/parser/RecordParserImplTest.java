package com.epam.training.hadoop.loganalyse.parser;

import com.epam.training.hadoop.loganalyse.domain.LogRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

public class RecordParserImplTest {

    private final static String EMPTY_RECORD = "";
    private final static String NULL_RECORD = null;
    private final static String INVALID_RECORD = "if54p1 - - [24/Apr/2011:04:10:19 -0400] \"Gdf5434524523ET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 20d0 fghsgafds56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    private final static String VALID_RECORD = "ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    private final static String INVALID_TIMEDATE_LOCALE_RECORD = "ip1 - - [24/Апр/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";

    private final static LogRecord RECORD_ENTITY = new LogRecord();

    public RecordParserImplTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        RECORD_ENTITY.setIp("ip1");
        RECORD_ENTITY.setSomeUnknownFields1("- -");
        RECORD_ENTITY.setDateTime(new DateTime(2011, 4, 24, 4, 10, 19).withZoneRetainFields(DateTimeZone.forOffsetHoursMinutes(-4, 0)));
        RECORD_ENTITY.setMethodName("GET");
        RECORD_ENTITY.setLocalLink("/~strabal/grease/photo1/97-13.jpg");
        RECORD_ENTITY.setProtocolName("HTTP/1.1");
        RECORD_ENTITY.setStatus(200);
        RECORD_ENTITY.setBytesTransfered(56928L);
        RECORD_ENTITY.setHostLink("-");
        RECORD_ENTITY.setMiscInfo("Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)");
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecord() {
        System.out.println("testParseRecord");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("record should be equal to expected", expResult, result);
    }
    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordIp() {
        System.out.println("testParseRecordIp");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("ip should be equal to expected", expResult.getIp(), result.getIp());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordSomeUnknownFields1() {
        System.out.println("testParseRecordSomeUnknownFields1");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("someUnknownFields1 should be equal to expected", expResult.getSomeUnknownFields1(), result.getSomeUnknownFields1());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordDateTime() {
        System.out.println("testParseRecordDateTime");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("dateTime should be equal to expected", expResult.getDateTime(), result.getDateTime());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordMethodName() {
        System.out.println("testParseRecordMethodName");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("methodName should be equal to expected", expResult.getMethodName(), result.getMethodName());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordLocalLink() {
        System.out.println("testParseRecordLocalLink");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("localLink should be equal to expected", expResult.getLocalLink(), result.getLocalLink());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordProtocolName() {
        System.out.println("testParseRecordProtocolName");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("protocolName should be equal to expected", expResult.getProtocolName(), result.getProtocolName());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordStatus() {
        System.out.println("testParseRecordStatus");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("status should be equal to expected", expResult.getStatus(), result.getStatus());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordBytesTransfered() {
        System.out.println("testParseRecordBytesTransfered");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("bytedTransfered should be equal to expected", expResult.getBytesTransfered(), result.getBytesTransfered());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordHostLink() {
        System.out.println("testParseRecordHostLink");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("hostLink should be equal to expected", expResult.getHostLink(), result.getHostLink());
    }

    /**
     * Test of parseRecord method, of class RecordParserImpl.
     */
    @Test
    public void testParseRecordMiscInfo() {
        System.out.println("testParseRecordMiscInfo");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        LogRecord expResult = RECORD_ENTITY;
        LogRecord result = instance.parseRecord(record);

        assertEquals("miscInfo should be equal to expected", expResult.getMiscInfo(), result.getMiscInfo());
    }

    /**
     * Test of isValidRecord method with empty record, of class
     * RecordParserImpl.
     */
    @Test
    public void testIsValidRecordWithEmptyRecord() {
        System.out.println("testIsValidRecordWithEmptyRecord");
        String record = EMPTY_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        boolean expResult = false;
        boolean result = instance.isValidRecord(record);
        assertEquals("empty record should not be a valid record", expResult, result);
    }

    /**
     * Test of isValidRecord method with empty record, of class
     * RecordParserImpl.
     */
    @Test
    public void testIsValidRecordWithNullRecord() {
        System.out.println("testIsValidRecordWithNullRecord");
        String record = NULL_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        boolean expResult = false;
        boolean result = instance.isValidRecord(record);
        assertEquals("null record should not be a valid record", expResult, result);
    }

    /**
     * Test of isValidRecord method with empty record, of class
     * RecordParserImpl.
     */
    @Test
    public void testIsValidRecordWithInvalidRecord() {
        System.out.println("testIsValidRecordWithInvalidRecord");
        String record = INVALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        boolean expResult = false;
        boolean result = instance.isValidRecord(record);
        assertEquals("invalid record should not be a valid record", expResult, result);
    }
    /**
     * Test of isValidRecord method with empty record, of class
     * RecordParserImpl.
     */
    @Test
    public void testIsValidRecordWithInvalidDateTimeLocaleRecord() {
        System.out.println("testIsValidRecordWithInvalidDateTimeLocaleRecord");
        String record = INVALID_TIMEDATE_LOCALE_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        boolean expResult = false;
        boolean result = instance.isValidRecord(record);
        assertEquals("invalid record should not be a valid record", expResult, result);
    }

    /**
     * Test of isValidRecord method with empty record, of class
     * RecordParserImpl.
     */
    @Test
    public void testIsValidRecordWithValidRecord() {
        System.out.println("testIsValidRecordWithValidRecord");
        String record = VALID_RECORD;
        RecordParserImpl instance = new RecordParserImpl();
        boolean expResult = true;
        boolean result = instance.isValidRecord(record);
        assertEquals("valid record should be a valid record", expResult, result);
    }

}
