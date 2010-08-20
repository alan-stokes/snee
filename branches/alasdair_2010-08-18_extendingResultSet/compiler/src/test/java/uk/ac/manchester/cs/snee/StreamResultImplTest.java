/****************************************************************************\
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://snee.cs.manchester.ac.uk/                                          *
*  http://code.google.com/p/snee                                             *
*                                                                            *
*  Release 1.x, 2009, under New BSD License.                                 *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/

package uk.ac.manchester.cs.snee;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.types.Duration;

public class StreamResultImplTest extends EasyMockSupport {
			
	final Output mockOutput = createMock(TaggedTuple.class);
	
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

	private StreamResult _resultSet;
	private String testQuery = "SELECT * FROM TestStream;";
	private LAF mockQEP = createMock(LAF.class);
	private ResultSetMetaData mockMetaData = 
		createMock(ResultSetMetaData.class);

	@Before
	public void setUp() throws Exception {
		_resultSet = new StreamResultImpl(testQuery, mockQEP) {
			protected ResultSetMetaData createMetaData(
					LAF queryPlan)
			throws SQLException {
				return mockMetaData;
			}
			
			protected List<Output> createDataStore() {
				List<Output> dataList = new ArrayList<Output>();
				dataList.add(mockOutput);//1
				dataList.add(mockOutput);//2
				dataList.add(mockOutput);//3
				dataList.add(mockOutput);//4
				dataList.add(mockOutput);//5
				dataList.add(mockOutput);//6
				dataList.add(mockOutput);//7
				dataList.add(mockOutput);//8
				dataList.add(mockOutput);//9
				dataList.add(mockOutput);//10
				return dataList;
			}
		}; 
		_resultSet.setCommand("SELECT * FROM TestStream;");
	}

	private void recordResultSet(int numResults) 
	throws SNEEException, SQLException {
		expect(mockMetaData.getColumnCount()).andReturn(2);
		Tuple mockTuple = createMock(Tuple.class);
		expect(((TaggedTuple) mockOutput).getTuple()).
			andReturn(mockTuple).times(numResults);
		expect(mockMetaData.getColumnLabel(1)).
			andReturn("attr").times(numResults);
		expect(mockMetaData.getColumnLabel(2)).
			andReturn("attr").times(numResults);
		Field mockField = createMock(Field.class);
		expect(mockTuple.getField("attr")).
			andReturn(mockField).times(numResults * 2);
		expect(mockField.getData()).
			andReturn("data").times(numResults * 2);
	}
	
	@After
	public void tearDown() throws Exception {
//		verifyAll();
	}
	
//	private void printResultSet(int queryId, Collection<Output> results) {
//		String msg = "";
//		for (Output result : results) {
//			msg += result + "\n";
//		}
//		System.out.println("Result for query " + queryId + ":\n" + msg);
//	}

	@Test
	public void testSize() {
		assertEquals(10, _resultSet.size());
	}
	
	@Test
	public void testGetCommand() {
		assertEquals("SELECT * FROM TestStream;",
				_resultSet.getCommand());
	}
	
	@Test
	public void testGetMetaData() {
		ResultSetMetaData metadata = _resultSet.getMetadata();
		assertNotNull(metadata);
	}
	
	@Test
	public void testGetResults() 
	throws SNEEException, SQLException {
		recordResultSet(10);
		replayAll();
		ResultSet results = _resultSet.getResults();
		assertNotNull(results);
		results.last();
		assertEquals(10, results.getRow());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResults_invalidCount() 
	throws SNEEException {
		_resultSet.getResults(42);
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_zeroCount() 
	throws SNEEException {
		_resultSet.getResults(0);
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_negativeCount() 
	throws SNEEException {
		/* Request a negative number of results should throw exception */
		_resultSet.getResults(-42);
	}
	
	@Test
	public void testGetResults_count() 
	throws SNEEException, SQLException {
		recordResultSet(2);
		replayAll();
		ResultSet results = _resultSet.getResults(2);
		results.last();
		assertEquals(2, results.getRow());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResults_zeroDuration() 
	throws SNEEException {
		/* Request a 0 duration of results should throw exception */
		_resultSet.getResults(new Duration(0));
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_negativeDuration() 
	throws SNEEException {
		/* Requesting a negative duration should throw exception */
		_resultSet.getResults(new Duration(-42));
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResults_invalidDuraction() 
	throws SNEEException {
		//Record
//		expect(mockQEP.getMetaData()).andReturn(mockQEPMetadata);
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).
			andReturn(currentTime - 10000).times(1).
			andReturn(currentTime);
		//Test
		replayAll();
		/* Request a larger duration of results than exist 
		 * should throw exception */
		_resultSet.getResults(new Duration(42, TimeUnit.DAYS));
		verifyAll();
	}
	
	@Test
	public void testGetResults_validSubDuraction() 
	throws SNEEException, SQLException {
		recordResultSet(3);
		/*
		 * Record result set of 10 seconds 
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).
			andReturn(currentTime - 10000).
			andReturn(currentTime).
			andReturn(currentTime - 10000).times(2).
			andReturn(currentTime - 8000).
			andReturn(currentTime - 6000).
			andReturn(currentTime - 4000);
		//Test
		replayAll();
		ResultSet results = 
			_resultSet.getResults(new Duration(5, TimeUnit.SECONDS));
		results.last();
		assertEquals(3, results.getRow());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndex_negativeIndex() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(-4);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndex_invalidIndex() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(400);
	}

	@Test
	public void testGetResultsFromIndex_zeroIndex() 
	throws SNEEException, SQLException {
		/* Returns entire result set */
		recordResultSet(10);
		replayAll();
		ResultSet result = _resultSet.getResultsFromIndex(0);
		result.last();
		assertEquals(10, result.getRow());
		verifyAll();
	}

	@Test
	public void testGetResultsFromIndex() 
	throws SNEEException, SQLException {
		recordResultSet(6);
		replayAll();
		ResultSet result = _resultSet.getResultsFromIndex(4);
		result.last();
		assertEquals(6, result.getRow());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultFromIndexSingleton()
	throws SNEEException {
		StreamResultImpl singletonResultSet = 
			new StreamResultImpl(testQuery, mockQEP) {
			
			protected ResultSetMetaData createMetaData(
					LAF queryPlan)
			throws SQLException {
				return mockMetaData;
			}
			
			protected List<Output> createDataStore() {
				List<Output> dataList = new ArrayList<Output>();
				dataList.add(mockOutput);//1
				return dataList;
			}
		}; 
		singletonResultSet.setCommand("SELECT * FROM TestStream;");
		singletonResultSet.getResultsFromIndex(1);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_negativeIndex() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(-4, 3);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_invalidIndex() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(400, 5);
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_invalidCount() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(2, 42);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_zeroCount() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(7, 0);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_negativeCount() 
	throws SNEEException {
		/* Request a negative number of results should throw exception */
		_resultSet.getResultsFromIndex(3, -42);
	}

	@Test
	public void testGetResultsFromIndexCount_zeroIndex() 
	throws SNEEException, SQLException {
		recordResultSet(7);
		replayAll();
		ResultSet result = _resultSet.getResultsFromIndex(0, 7);
		result.last();
		assertEquals(7, result.getRow());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_invalidCountIndex() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(8, 5);
	}

	@Test
	public void testGetResultsFromIndexCount() 
	throws SNEEException, SQLException {
		recordResultSet(2);
		replayAll();
		ResultSet results = _resultSet.getResultsFromIndex(4, 2);
		results.last();
		assertEquals(2, results.getRow());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexDuration() 
	throws SNEEException {
		_resultSet.getResultsFromIndex(3, new Duration(3000));
	}

	@Ignore@Test
	public void testGetResultsFromIndex_duration() 
	throws SNEEException {
		//FIXME: Implement testGetResultsFromIndex_duration tests
		fail();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestamp_invalidFutureTimestamp() 
	throws SNEEException {
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 500000);
		_resultSet.getResultsFromTimestamp(ts);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestamp_invalidOldTimestamp() 
	throws SNEEException {
		/*
		 * Record 10 second result set
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 500000);
		_resultSet.getResultsFromTimestamp(ts);
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestamp() 
	throws SNEEException, SQLException {
		recordResultSet(5);
		/*
		 * Record 10 second result set with tuples 1 second apart
		 * -> only 5 are valid for the answer set
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).
			andReturn(currentTime - 10000).times(2).
			andReturn(currentTime - 9000).
			andReturn(currentTime - 8000).
			andReturn(currentTime - 7000).
			andReturn(currentTime - 6000).
			andReturn(currentTime - 5000).
			andReturn(currentTime - 4000).
			andReturn(currentTime - 3000).
			andReturn(currentTime - 2000).
			andReturn(currentTime - 1000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 5000);
		ResultSet result = _resultSet.getResultsFromTimestamp(ts);
		result.last();
		assertEquals(5, result.getRow());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_invalidFutureTimestamp() 
	throws SNEEException {
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 500000);
		_resultSet.getResultsFromTimestamp(ts, 3);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_invalidOldTimestamp() 
	throws SNEEException {
		/*
		 * Record 10 second result set 
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 500000);
		_resultSet.getResultsFromTimestamp(ts, 3);
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_invalidCount() 
	throws SNEEException {
		long currentTime = System.currentTimeMillis();
		Timestamp ts = new Timestamp(currentTime + 5000);
		_resultSet.getResultsFromTimestamp(ts, 49292);
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_negativeCount() 
	throws SNEEException {
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		_resultSet.getResultsFromTimestamp(ts, -92);
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_zeroCount() 
	throws SNEEException {
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		_resultSet.getResultsFromTimestamp(ts, 0);
	}
	
	@Test
	public void testGetResultsFromTimestampCount() 
	throws SNEEException, SQLException {
		recordResultSet(3);
		/*
		 * Record 10 second result set with tuples 1 second apart
		 * ->  4 are valid for the answer set, but only 3 should be returned
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).
			andReturn(currentTime - 10000).times(2).
			andReturn(currentTime - 9000).
			andReturn(currentTime - 8000).
			andReturn(currentTime - 7000).
			andReturn(currentTime - 6000).
			andReturn(currentTime - 5000).
			andReturn(currentTime - 4000).
			andReturn(currentTime - 3000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 5000);
		ResultSet result = _resultSet.getResultsFromTimestamp(ts, 3);
		result.last();
		assertEquals(3, result.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestampCount_fullSet() 
	throws SNEEException, SQLException {
		recordResultSet(10);
		/*
		 * Record 10 second result set with tuples 1 second apart
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).
			andReturn(currentTime - 10000).times(2).
			andReturn(currentTime - 9000).
			andReturn(currentTime - 8000).
			andReturn(currentTime - 7000).
			andReturn(currentTime - 6000).
			andReturn(currentTime - 5000).
			andReturn(currentTime - 4000).
			andReturn(currentTime - 3000).
			andReturn(currentTime - 2000).
			andReturn(currentTime - 1000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 10000);
		ResultSet result = _resultSet.getResultsFromTimestamp(ts, 10);
		result.last();
		assertEquals(10, result.getRow());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_invalidFutureTimestamp() 
	throws SNEEException {
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 500000);
		_resultSet.getResultsFromTimestamp(ts, new Duration(45000));
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_invalidOldTimestamp() 
	throws SNEEException {
		/*
		 * Record 10 second result set
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(System.currentTimeMillis() - 500000);
		_resultSet.getResultsFromTimestamp(ts, new Duration(30000));
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_zeroDuration() 
	throws SNEEException {
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		/* Request a 0 duration of results should throw exception */
		_resultSet.getResultsFromTimestamp(ts, new Duration(0));
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_negativeDuration() 
	throws SNEEException {
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		/* Requesting a negative duration should throw exception */
		_resultSet.getResultsFromTimestamp(ts, new Duration(-42));
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_invalidDuraction() 
	throws SNEEException {
		//Record result set of 10 seconds
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000).times(1).andReturn(currentTime);
		//Test
		replayAll();
		/* Request a larger duration of results than exist 
		 * should throw exception */
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		_resultSet.getResultsFromTimestamp(ts, new Duration(42, TimeUnit.DAYS));
	}
	
	@Test
	public void testGetResultsFromTimestampDuration_validSubDuraction() 
	throws SNEEException, SQLException {
		recordResultSet(2);
		/*
		 * Record 10 second result set with tuples second apart
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).
			andReturn(currentTime - 10000).times(2).
			andReturn(currentTime - 1000).
			andReturn(currentTime - 10000).
			andReturn(currentTime - 9000).
			andReturn(currentTime - 8000).
			andReturn(currentTime - 7000).
			andReturn(currentTime - 6000).
			andReturn(currentTime - 5000).
			andReturn(currentTime - 4000).
			andReturn(currentTime - 3000).
			andReturn(currentTime - 2000).
			andReturn(currentTime - 1000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 5000);
		ResultSet results = 
			_resultSet.getResultsFromTimestamp(ts, 
					new Duration(2, TimeUnit.SECONDS));
		results.last();
		assertEquals(2, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetNewestResults_fullCount() 
	throws SNEEException, SQLException {
		recordResultSet(10);
		replayAll();
		/* Request the full set back */
		ResultSet results = _resultSet.getNewestResults(10);
		results.last();
		assertEquals(10, results.getRow());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetNewestResults_invalidCount() 
	throws SNEEException {
		/* Request a larger number of results than exist 
		 * should throw exception */
		_resultSet.getNewestResults(45);
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_zeroCount() 
	throws SNEEException {
		/* Request a 0 number of results should throw exception */
		_resultSet.getNewestResults(0);
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_negativeCount() 
	throws SNEEException {
		/* Request a negative number of results should throw exception */
		_resultSet.getNewestResults(-42);
	}
	
	@Test
	public void testGetNewestResults_count() 
	throws SNEEException, SQLException {
		/*
		 * Test resultset contains 10 tuples
		 * Answer set should only contain the most recent 2
		 */
		int count = 2;
		recordResultSet(count);
		replayAll();
		ResultSet results = _resultSet.getNewestResults(count);
		results.last();
		assertEquals(count, results.getRow());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetNewestResults_zeroDuration() 
	throws SNEEException {
		/* Request a 0 duration of results should throw exception */
		_resultSet.getNewestResults(new Duration(0));
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_negativeDuration() 
	throws SNEEException {
		/* Requesting a negative duration should throw exception */
		_resultSet.getNewestResults(new Duration(-42));
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_invalidDuraction() 
	throws SNEEException {
		/* Request a larger duration of results than exist 
		 * should throw exception */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 300000);
		replayAll();
		_resultSet.getNewestResults(new Duration(42, TimeUnit.DAYS));
		verifyAll();
	}
	
	@Test
	public void testGetNewestResults_validSubDuraction() 
	throws SNEEException, SQLException {
		recordResultSet(5);
		/* 
		 * Record a tuple set with a tuple being published every second
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 600000);
		expect(mockOutput.getEvalTime()).andReturn(currentTime);
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 1000);
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 2000);
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 3000);
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 4000);
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 5000);
		//Test
		replayAll();
		ResultSet results = 
			_resultSet.getNewestResults(new Duration(5, TimeUnit.SECONDS));
		results.last();
		assertEquals(5, results.getRow());
		verifyAll();
	}
	
}
