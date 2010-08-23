package uk.ac.manchester.cs.snee;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.StringBufferInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

public class StreamResultSetTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				StreamResultImplTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private ResultSetMetaData mockMetadata =
		createMock(ResultSetMetaData.class);
	private TaggedTuple mockTT =
		createMock(TaggedTuple.class);

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	private void recordResultSet(int numResults, Object data) 
	throws SNEEException, SQLException {
		expect(mockMetadata.getColumnCount()).andReturn(2);
		if (numResults > 0) {
			Tuple mockTuple = createMock(Tuple.class);
			expect(mockTT.getTuple()).
				andReturn(mockTuple).times(numResults);
			expect(mockMetadata.getColumnLabel(1)).
				andReturn("attr1").times(numResults);
			expect(mockMetadata.getColumnLabel(2)).
				andReturn("attr2").times(numResults);
			Field mockField = createMock(Field.class);
			expect(mockTuple.getField("attr1")).
				andReturn(mockField).times(numResults);
			expect(mockTuple.getField("attr2")).
				andReturn(mockField).times(numResults);
			expect(mockField.getData()).
				andReturn(data).times(numResults * 2);
		}
	}

	@Test
	public void testAbsolute_0() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.absolute(0);
		assertEquals(false, response);
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testAbsolute_1() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.absolute(1);
		assertEquals(true, response);
		assertEquals(1, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testAbsolute_4() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.absolute(4);
		assertEquals(false, response);
		assertEquals(4, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testAbsolute_negative2() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.absolute(-2);
		assertEquals(true, response);
		assertEquals(2, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testAbsolute_negative4() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.absolute(-4);
		assertEquals(false, response);
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testAfterLast_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.afterLast();
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testAfterLast_data() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.afterLast();
		assertEquals(4, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testBeforeFirst_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.beforeFirst();
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testBeforeFirst_data() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.beforeFirst();
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testBeforeFirst_dataMoveCursor() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(2);
		resultSet.beforeFirst();
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testDeleteRow()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.deleteRow();
		verifyAll();
	}

	@Test
	public void testFindColumn_exists() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		int column = resultSet.findColumn("attr1");
		assertEquals(1, column);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testFindColumn_notExists() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		expect(mockMetadata.getColumnLabel(2)).andReturn("attr2");
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.findColumn("attr");
		verifyAll();
	}

	@Test
	public void testFirst_data() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.first();
		assertEquals(true, response);
		verifyAll();
	}

	@Test
	public void testFirst_noData() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.first();
		assertEquals(false, response);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetArrayInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getArray(3);
		verifyAll();		
	}

	@Test(expected=SQLException.class)
	public void testGetArrayString()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getArray("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetAsciiStreamInt()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getAsciiStream(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetAsciiStreamString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getAsciiStream("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBigDecimalInt_nonDecimal() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new BigDecimal(3.1280));
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getBigDecimal(1);
		verifyAll();
	}

	@Test
	public void testGetBigDecimalInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new BigDecimal(3.1280));
		expect(mockMetadata.getColumnType(1)).andReturn(Types.DECIMAL);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		BigDecimal answer = resultSet.getBigDecimal(1);
		assertEquals(new BigDecimal(3.1280), answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBigDecimalString_nonDecimal() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new BigDecimal(3.1280));
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getBigDecimal("attr1");
		verifyAll();
	}
	
	@Test
	public void testGetBigDecimalString_decimal() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new BigDecimal(3.1280));
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr2");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.DECIMAL);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		BigDecimal answer = resultSet.getBigDecimal("attr2");
		assertEquals(new BigDecimal(3.1280), answer);
		verifyAll();
	}

	//Not supporting the depricated method
	@Test(expected=SQLException.class)
	public void testGetBigDecimalIntInt()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBigDecimal(1, 0);
		verifyAll();
	}

	//Not supporting the depricated method
	@Test(expected=SQLException.class)
	public void testGetBigDecimalStringInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBigDecimal("", 0);
		verifyAll();
	}
	
	@Test(expected=SQLException.class)
	public void testGetBinaryStreamInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBinaryStream(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBinaryStreamString()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBinaryStream("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBlobInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBlob(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBlobString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBlob("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBooleanInt_nonBoolean() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		expect(mockMetadata.getColumnType(1)).andReturn(Types.DATE);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBoolean(1);
		verifyAll();
	}

	@Test
	public void testGetBooleanInt_BooleanTrue() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, true);
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		boolean answer = resultSet.getBoolean(1);
		assertEquals(true, answer);
		verifyAll();
	}

	@Test
	public void testGetBooleanInt_BooleanFalse() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, false);
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(3);
		boolean answer = resultSet.getBoolean(1);
		assertEquals(false, answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBooleanString_nonBoolean() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr2");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.DATE);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBoolean("attr2");
		verifyAll();
	}

	@Test
	public void testGetBooleanString_BooleanTrue() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, true);
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		boolean answer = resultSet.getBoolean("attr1");
		assertEquals(true, answer);
		verifyAll();
	}

	@Test
	public void testGetBooleanString_BooleanFalse() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, false);
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr2");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(3);
		boolean answer = resultSet.getBoolean("attr2");
		assertEquals(false, answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetByteInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getByte(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetByteString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getByte("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBytesInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBytes(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetBytesString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getBytes("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetCharacterStreamInt()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getCharacterStream(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetCharacterStreamString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getCharacterStream("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetClobInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getClob(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetClobString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getClob("");
		verifyAll();
	}

	@Test
	public void testGetConcurrency() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(ResultSet.CONCUR_READ_ONLY, 
				resultSet.getConcurrency());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetCursorName()
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getDate(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetDateInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getDate(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetDateString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getDate("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetDateIntCalendar()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getDate(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetDateStringCalendar()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getDate("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetDoubleInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getDouble(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetDoubleString()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getDouble("");
		verifyAll();
	}

	@Test
	public void testGetFetchDirection() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(ResultSet.FETCH_UNKNOWN, 
				resultSet.getFetchDirection());
		verifyAll();
	}

	@Test
	public void testGetFetchSize() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(0, resultSet.getFetchSize());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetFloatInt_nonFloat() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Float(3.5));
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getFloat(1);
		verifyAll();
	}

	@Test
	public void testGetFloatInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Float(3.5));
		expect(mockMetadata.getColumnType(1)).andReturn(Types.FLOAT);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		float answer = resultSet.getFloat(1);
		assertEquals(true, 3.5 == answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetFloatString_nonFloat() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Float(3.5));
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getFloat("attr1");
		verifyAll();
	}
	
	@Test
	public void testGetFloatString_float() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Float(3.5));
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr2");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.FLOAT);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		float answer = resultSet.getFloat("attr2");
		assertEquals(true, 3.5 == answer);
		verifyAll();
	}

	@Test
	public void testGetHoldability()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT,
				resultSet.getHoldability());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetIntInt_nonInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Integer(42));
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getInt(1);
		verifyAll();
	}

	@Test
	public void testGetIntInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Integer(42));
		expect(mockMetadata.getColumnType(1)).andReturn(Types.INTEGER);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		int answer = resultSet.getInt(1);
		assertEquals(true, 42 == answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetIntString_nonInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Integer(42));
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getInt("attr1");
		verifyAll();
	}
	
	@Test
	public void testGetIntString_int() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Integer(42));
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr2");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.INTEGER);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		int answer = resultSet.getInt("attr2");
		assertEquals(true, 42 == answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetLongInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getLong(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetLongString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getLong("");
		verifyAll();
	}

	@Test
	public void testGetMetaData()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		ResultSetMetaData metaData = resultSet.getMetaData();
		assertEquals(mockMetadata, metaData);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetNCharacterStreamInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getNCharacterStream(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetNCharacterStreamString()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getNCharacterStream("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetNClobInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getNClob(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetNClobString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getNClob("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetNStringInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getNString(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetNStringString()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getNString("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetObjectInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getObject(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetObjectString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getObject("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetObjectIntMapOfStringClassOfQ() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getObject(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetObjectStringMapOfStringClassOfQ() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getObject("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetRefInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getRef(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetRefString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getRef("");
		verifyAll();
	}

	@Test
	public void testGetRow() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testGetRow_data() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(0, resultSet.getRow());
		resultSet.absolute(2);
		assertEquals(2, resultSet.getRow());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetRowIdInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getSQLXML(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetRowIdString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getSQLXML(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetSQLXMLInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getSQLXML(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetSQLXMLString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getSQLXML("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetShortInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getShort(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetShortString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getShort("");
		verifyAll();
	}

	@Test
	public void testGetStatement() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		Statement statement = resultSet.getStatement();
		assertNull(statement);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetStringInt_nonString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, "data");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getString(1);
		verifyAll();
	}

	@Test
	public void testGetStringInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, "data");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.VARCHAR);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		String answer = resultSet.getString(1);
		assertEquals("data", answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetStringString_nonString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, new Integer(42));
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getString("attr1");
		verifyAll();
	}
	
	@Test
	public void testGetStringString_int() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, "data");
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr2");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.VARCHAR);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		String answer = resultSet.getString("attr2");
		assertEquals("data", answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimeInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getTime(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimeString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getTime("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimeIntCalendar() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getTime(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimeStringCalendar() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getTime("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimestampInt_nonString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		Long t = new Long("1282315575176");
		Timestamp ts = new Timestamp(t.longValue());
		recordResultSet(3, ts);
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getTimestamp(1);
		verifyAll();
	}

	@Test
	public void testGetTimestampInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		Long t = new Long("1282315575176");
		Timestamp ts = new Timestamp(t.longValue());
		recordResultSet(3, ts);
		expect(mockMetadata.getColumnType(1)).andReturn(Types.TIMESTAMP);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		Timestamp answer = resultSet.getTimestamp(1);
		assertEquals(ts, answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimestampString_nonString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		Long t = new Long("1282315575176");
		Timestamp ts = new Timestamp(t.longValue());
		recordResultSet(3, ts);
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr1");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.BOOLEAN);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		resultSet.getTimestamp("attr1");
		verifyAll();
	}
	
	@Test
	public void testGetTimestampString_int() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		Long t = new Long("1282315575176");
		Timestamp ts = new Timestamp(t.longValue());
		recordResultSet(3, ts);
		expect(mockMetadata.getColumnCount()).andReturn(2);
		expect(mockMetadata.getColumnLabel(1)).andReturn("attr2");
		expect(mockMetadata.getColumnType(1)).andReturn(Types.TIMESTAMP);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		Timestamp answer = resultSet.getTimestamp("attr2");
		assertEquals(true, ts == answer);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimestampIntCalendar() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getTimestamp(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetTimestampStringCalendar() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getTimestamp("", null);
		verifyAll();
	}

	@Test
	public void testGetType() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE,
				resultSet.getType());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetURLInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getURL(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetURLString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getURL("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetUnicodeStreamInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getUnicodeStream(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testGetUnicodeStreamString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.getUnicodeStream("");
		verifyAll();
	}

	@Test
	public void testGetWarnings() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertNull(resultSet.getWarnings());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testInsertRow() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.insertRow();
		verifyAll();
	}

	@Test
	public void testIsAfterLast_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.isAfterLast());
		verifyAll();
	}

	@Test
	public void testIsAfterLast_dataNegative() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.isAfterLast();
		assertEquals(false, response);
		verifyAll();
	}

	@Test
	public void testIsAfterLast_dataTrue() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(4);
		boolean response = resultSet.isAfterLast();
		assertEquals(true, response);
		verifyAll();
	}

	@Test
	public void testIsBeforeFirst_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.isBeforeFirst());
		verifyAll();
	}

	@Test
	public void testIsBeforeFirst_dataNegative() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(3);
		boolean response = resultSet.isBeforeFirst();
		assertEquals(false, response);
		verifyAll();
	}

	@Test
	public void testIsBeforeFirst_dataTrue() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		boolean response = resultSet.isBeforeFirst();
		assertEquals(true, response);
		verifyAll();
	}

	@Test
	public void testIsClosed() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.isClosed());
		verifyAll();
	}

	@Test
	public void testIsFirst_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.isFirst());
		verifyAll();
	}

	@Test
	public void testIsFirst_dataFalse() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(3);
		assertEquals(false, resultSet.isFirst());
		verifyAll();
	}

	@Test
	public void testIsFirst_dataBeforeFirst() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.isFirst());
		verifyAll();
	}

	@Test
	public void testIsFirst_dataTrue() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		assertEquals(true, resultSet.isFirst());
		verifyAll();
	}


	@Test
	public void testIsLast_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.isLast());
		verifyAll();
	}

	@Test
	public void testIsLast_dataFalse() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		assertEquals(false, resultSet.isLast());
		verifyAll();
	}

	@Test
	public void testIsLast_dataAfterLast() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(4);
		assertEquals(false, resultSet.isLast());
		verifyAll();
	}

	@Test
	public void testIsLast_dataTrue() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(3);
		assertEquals(true, resultSet.isLast());
		verifyAll();
	}
	
	@Test
	public void testLast_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.last());
		verifyAll();
	}
	
	@Test
	public void testLast_data() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(true, resultSet.last());
		verifyAll();
	}
	
	@Test(expected=SQLException.class)
	public void testMoveToCurrentRow() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.moveToCurrentRow();
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testMoveToInsertRow()  
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.moveToInsertRow();
		verifyAll();
	}

	@Test
	public void testNext_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.next());
		assertEquals(true, resultSet.isAfterLast());
		verifyAll();
	}

	@Test
	public void testNext_dataBeforeFirst() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(true, resultSet.next());
		assertEquals(1, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testNext_dataOnRow() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(2);
		assertEquals(true, resultSet.next());
		assertEquals(3, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testNext_dataOnLast() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.last();
		assertEquals(false, resultSet.next());
		assertEquals(true, resultSet.isAfterLast());
		verifyAll();
	}

	@Test
	public void testPrevious_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.previous());
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testPrevious_dataBeforeFirst() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.previous());
		assertEquals(true, resultSet.isBeforeFirst());
		verifyAll();
	}

	@Test
	public void testPrevious_dataAfterLast() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.afterLast();
		assertEquals(true, resultSet.previous());
		assertEquals(3, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testPrevious_dataOnRow() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(2);
		assertEquals(true, resultSet.previous());
		assertEquals(1, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testPrevious_dataOnFirst() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.first();
		assertEquals(false, resultSet.previous());
		assertEquals(true, resultSet.isBeforeFirst());
		verifyAll();
	}

	@Test(expected=SQLFeatureNotSupportedException.class)
	public void testRefreshRow() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.refreshRow();
		verifyAll();
	}

	@Test
	public void testRelativePos_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.relative(1));
		assertEquals(true, resultSet.isAfterLast());
		verifyAll();
	}

	@Test
	public void testRelativePos_dataBeforeFirst() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(true, resultSet.relative(1));
		assertEquals(1, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testRelativePos_dataOnRow() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(2);
		assertEquals(true, resultSet.relative(1));
		assertEquals(3, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testRelativePos_dataOnLast() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.last();
		assertEquals(false, resultSet.relative(1));
		assertEquals(true, resultSet.isAfterLast());
		verifyAll();
	}

	@Test
	public void testRelativePos_dataOnRowMove2Valid() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(true, resultSet.relative(2));
		assertEquals(2, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testRelativePos_dataOnRowMove2NotValid() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(2);
		assertEquals(false, resultSet.relative(2));
		assertEquals(true, resultSet.isAfterLast());
		verifyAll();
	}

	@Test
	public void testRelativeNeg_noData() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.relative(-1));
		assertEquals(0, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testRelativeNeg_dataBeforeFirst() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.relative(-1));
		assertEquals(true, resultSet.isBeforeFirst());
		verifyAll();
	}

	@Test
	public void testRelativeNeg_dataAfterLast() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.afterLast();
		assertEquals(true, resultSet.relative(-1));
		assertEquals(3, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testRelativeNeg_dataOnRow() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(2);
		assertEquals(true, resultSet.relative(-1));
		assertEquals(1, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testRelativeNeg_dataOnFirst() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.first();
		assertEquals(false, resultSet.relative(-1));
		assertEquals(true, resultSet.isBeforeFirst());
		verifyAll();
	}

	@Test
	public void testRelativeNeg_dataOnRowMove2Valid() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(3);
		assertEquals(true, resultSet.relative(-2));
		assertEquals(1, resultSet.getRow());
		verifyAll();
	}

	@Test
	public void testRelativeNeg_dataOnRowMove2NotValid() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.absolute(1);
		assertEquals(false, resultSet.relative(-2));
		assertEquals(true, resultSet.isBeforeFirst());
		verifyAll();
	}

	@Test(expected=SQLFeatureNotSupportedException.class)
	public void testRowDeleted() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.rowDeleted();
		verifyAll();
	}

	@Test(expected=SQLFeatureNotSupportedException.class)
	public void testRowInserted() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.rowDeleted();
		verifyAll();
	}

	@Test(expected=SQLFeatureNotSupportedException.class)
	public void testRowUpdated() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.rowDeleted();
		verifyAll();
	}

	@Test
	public void testSetFetchDirection_forward() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
		assertEquals(ResultSet.FETCH_UNKNOWN, 
				resultSet.getFetchDirection());
		verifyAll();
	}

	@Test
	public void testSetFetchDirection_reverse() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
		assertEquals(ResultSet.FETCH_UNKNOWN, 
				resultSet.getFetchDirection());
		verifyAll();
	}

	@Test
	public void testSetFetchDirection_unknown() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.setFetchDirection(ResultSet.FETCH_UNKNOWN);
		assertEquals(ResultSet.FETCH_UNKNOWN, 
				resultSet.getFetchDirection());
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testSetFetchSize_negSize() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.setFetchSize(-3);
		verifyAll();
	}

	@Test
	public void testSetFetchSize_posSize() 
	throws SNEEException, SQLException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.setFetchSize(4);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateArrayIntArray()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateArray(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateArrayStringArray() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateArray("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateAsciiStreamIntInputStream() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateAsciiStream(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateAsciiStreamStringInputStream()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateAsciiStream("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateAsciiStreamIntInputStreamInt()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateAsciiStream(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateAsciiStreamStringInputStreamInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateAsciiStream("", null, 0);		
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateAsciiStreamIntInputStreamLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateAsciiStream(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateAsciiStreamStringInputStreamLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateAsciiStream("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBigDecimalIntBigDecimal() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBigDecimal(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBigDecimalStringBigDecimal() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBigDecimal("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBinaryStreamIntInputStream() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBinaryStream(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBinaryStreamStringInputStream()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBinaryStream("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBinaryStreamIntInputStreamInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBinaryStream(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBinaryStreamStringInputStreamInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBinaryStream("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBinaryStreamIntInputStreamLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBinaryStream(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBinaryStreamStringInputStreamLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBinaryStream("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBlobIntBlob()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBlob(1, new SerialBlob(new byte[3]));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBlobStringBlob() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBlob("", new SerialBlob(new byte[3]));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBlobIntInputStream() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBlob(1, new StringBufferInputStream(""));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBlobStringInputStream() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBlob("", new StringBufferInputStream(""));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBlobIntInputStreamLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBlob(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBlobStringInputStreamLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBlob("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBooleanIntBoolean() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBoolean(1, false);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBooleanStringBoolean() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBoolean("", true);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateByteIntByte()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateByte(1, (byte) 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateByteStringByte() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateByte("", (byte) 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBytesIntByteArray() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBytes(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateBytesStringByteArray() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateBytes("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateCharacterStreamIntReader() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateCharacterStream(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateCharacterStreamStringReader() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateCharacterStream("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateCharacterStreamIntReaderInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateCharacterStream(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateCharacterStreamStringReaderInt()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateCharacterStream("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateCharacterStreamIntReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateCharacterStream(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateCharacterStreamStringReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateCharacterStream("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateClobIntClob()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateClob(1, new SerialClob(new char[3]));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateClobStringClob() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateClob("", new SerialClob(new char[3]));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateClobIntReader()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateClob(1, new StringReader(""));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateClobStringReader() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateClob("", new StringReader(""));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateClobIntReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateClob(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateClobStringReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateClob("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateDateIntDate()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateDate(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateDateStringDate() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateDate("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateDoubleIntDouble() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateDouble(1, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateDoubleStringDouble() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateDouble("", 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateFloatIntFloat()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateFloat(1, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateFloatStringFloat() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateFloat("", 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateIntIntInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateInt(1, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateIntStringInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateInt("", 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateLongIntLong()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateLong(1, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateLongStringLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateLong("", 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNCharacterStreamIntReader() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNCharacterStream(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNCharacterStreamStringReader() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNCharacterStream("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNCharacterStreamIntReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNCharacterStream(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNCharacterStreamStringReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNCharacterStream("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNClobIntNClob() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNClob(1, createMock(NClob.class));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNClobStringNClob() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNClob("", createMock(NClob.class));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNClobIntReader() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNClob(1, new StringReader(""));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNClobStringReader() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNClob("", new StringReader(""));
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNClobIntReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNClob(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNClobStringReaderLong() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNClob("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNStringIntString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNString(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNStringStringString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNString("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNullInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNull(1);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateNullString()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateNull("");
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateObjectIntObject() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateObject(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateObjectStringObject() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateObject("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateObjectIntObjectInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateObject(1, null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateObjectStringObjectInt() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateObject("", null, 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateRefIntRef() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateRef(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateRefStringRef()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateRef("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateRow() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateRow();
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateRowIdIntRowId() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateRowId(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateRowIdStringRowId() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateRowId("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateSQLXMLIntSQLXML() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateSQLXML(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateSQLXMLStringSQLXML() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateSQLXML("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateShortIntShort() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateShort(1, (short) 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateShortStringShort() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateShort("", (short) 0);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateStringIntString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateString(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateStringStringString() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateString("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateTimeIntTime()
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateTime(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateTimeStringTime() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateTime("", null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateTimestampIntTimestamp() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateTimestamp(1, null);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUpdateTimestampStringTimestamp() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		recordResultSet(0, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.updateTimestamp("", null);
		verifyAll();
	}

	@Test
	public void testWasNull() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.wasNull());
		verifyAll();
	}

	@Test
	public void testIsWrapperFor_resultSet() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(true, resultSet.isWrapperFor(ResultSet.class));
		verifyAll();
	}

	@Test
	public void testIsWrapperFor_StreamResult() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		assertEquals(false, resultSet.isWrapperFor(StreamResult.class));
		verifyAll();
	}

	@Test
	public void testUnwrap_ResultSet() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.unwrap(ResultSet.class);
		verifyAll();
	}

	@Test(expected=SQLException.class)
	public void testUnwrap_StreamResult() 
	throws SQLException, SNEEException {
		List<TaggedTuple> dataList = new ArrayList<TaggedTuple>();
		dataList.add(mockTT);//1
		dataList.add(mockTT);//2
		dataList.add(mockTT);//3
		recordResultSet(3, null);
		replayAll();
		StreamResultSet resultSet = 
			new StreamResultSet(mockMetadata, dataList);
		resultSet.unwrap(StreamResult.class);
		verifyAll();
	}

}
