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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.common.CircularArray;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;
import uk.ac.manchester.cs.snee.types.Duration;

public class ResultStoreImplTest extends EasyMockSupport {
			
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				ResultStoreImplTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private ResultStore _resultStore;
	private String testQuery = "SELECT * FROM TestStream;";
	private QueryExecutionPlan mockQEP = 
		createMock(QueryExecutionPlan.class);
	private ResultSetMetaData mockMetaData = 
		createMock(ResultSetMetaData.class);
	private ResultSet mockResultSet = 
		createMock(ResultSet.class);
	private Output mockOutput;

	public void setUpTupleStream() 
	throws SNEEException, SNEEConfigurationException {
		mockOutput = createMock(TaggedTuple.class);
		_resultStore = new ResultStoreImpl(testQuery, mockQEP) {
			protected ResultSetMetaData createMetaData(
					QueryExecutionPlan queryPlan)
			throws SQLException {
				return mockMetaData;
			}
			
			protected CircularArray<Output> createDataStore() {
				CircularArray<Output> dataList = 
					new CircularArray<Output>(10);
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
			
			protected String setQueryId(QueryExecutionPlan queryPlan) {
				return "1";
			}
		}; 
	}

	public void setUpWindowStream() 
	throws SNEEException, SNEEConfigurationException {
		mockOutput = createMock(Window.class);
		_resultStore = new ResultStoreImpl(testQuery, mockQEP) {
			protected ResultSetMetaData createMetaData(
					QueryExecutionPlan queryPlan)
			throws SQLException {
				return mockMetaData;
			}
			
			protected CircularArray<Output> createDataStore() {
				CircularArray<Output> dataList = 
					new CircularArray<Output>(10);
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
			
			protected ResultSet createRS(List<Tuple> tuples) 
			throws SQLException, SNEEException {
//				System.out.println("Returning mock result set");
				return mockResultSet;
			}
			
			protected String setQueryId(QueryExecutionPlan queryPlan) {
				return "1";
			}
			
		}; 
	}

	private void recordTupleStreamResultSet(int numResults) 
	throws SNEEException, SQLException {
		expect(mockMetaData.getColumnCount()).andReturn(2);
		Tuple mockTuple = createMock(Tuple.class);
		expect(((TaggedTuple) mockOutput).getTuple()).
			andReturn(mockTuple).times(numResults);
		recordTuple(numResults, mockTuple);
	}
	
	private void recordTuple(int numResults, Tuple mockTuple)
	throws SNEEException {
		EvaluatorAttribute mockAttr = 
			createMock(EvaluatorAttribute.class);
		expect(mockTuple.getAttribute(0)).
			andReturn(mockAttr).times(numResults);
		expect(mockTuple.getAttribute(1)).
			andReturn(mockAttr).times(numResults);
		expect(mockAttr.getData()).
			andReturn("data").times(numResults * 2);
	}

	private void recordWindowStreamResultSet(int numResults)
	throws SNEEException, SQLException {
		List<Tuple> mockTupleList = createMock(List.class);
		expect(((Window) mockOutput).getTuples()).
			andReturn(mockTupleList).times(numResults);
	}
	
	@After
	public void tearDown() throws Exception {
	}
	
//	private void printResultSet(int queryId, Collection<Output> results) {
//		String msg = "";
//		for (Output result : results) {
//			msg += result + "\n";
//		}
//		System.out.println("Result for query " + queryId + ":\n" + msg);
//	}

	@Test
	public void testSize() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		assertEquals(10, _resultStore.size());
	}
	
	@Test
	public void testGetCommand() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.setCommand("SELECT * FROM TestStream;");
		assertEquals("SELECT * FROM TestStream;",
				_resultStore.getCommand());
	}
	
	@Test
	public void testGetMetaData() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		ResultSetMetaData metadata = _resultStore.getMetadata();
		assertNotNull(metadata);
	}
	
	@Test
	public void testGetResults_streamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(10);
		replayAll();
		List<ResultSet> resultSets = _resultStore.getResults();
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(10, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResults_streamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(10);
		replayAll();
		List<ResultSet> results = _resultStore.getResults();
		assertNotNull(results);
		assertEquals(10, results.size());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResults_invalidCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResults(42);
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_zeroCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResults(0);
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_negativeCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a negative number of results should throw exception */
		_resultStore.getResults(-42);
	}
	
	@Test
	public void testGetResults_countTupleStream() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(2);
		replayAll();
		List<ResultSet> resultSets = _resultStore.getResults(2);
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(2, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResults_countWindowStream() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(2);
		replayAll();
		List<ResultSet> results = _resultStore.getResults(2);
		assertEquals(2, results.size());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResults_zeroDuration() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a 0 duration of results should throw exception */
		_resultStore.getResults(new Duration(0));
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_negativeDuration() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Requesting a negative duration should throw exception */
		_resultStore.getResults(new Duration(-42));
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResults_invalidDuraction() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
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
		_resultStore.getResults(new Duration(42, TimeUnit.DAYS));
		verifyAll();
	}
	
	@Test
	public void testGetResults_validSubDuractionStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(3);
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
		List<ResultSet> resultSets = _resultStore.getResults(
				new Duration(5, TimeUnit.SECONDS));
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(3, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResults_validSubDuractionStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(3);
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
		replayAll();
		List<ResultSet> results = _resultStore.getResults(
				new Duration(5, TimeUnit.SECONDS));
		assertEquals(3, results.size());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndex_negativeIndex() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(-4);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndex_invalidIndex() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(400);
	}

	@Test
	public void testGetResultsFromIndex_zeroIndexStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		/* Returns entire result set */
		recordTupleStreamResultSet(10);
		replayAll();
		List<ResultSet> results = _resultStore.getResultsFromIndex(0);
		assertEquals(1, results.size());
		ResultSet result = results.get(0);
		result.last();
		assertEquals(10, result.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromIndex_zeroIndexStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		/* Returns entire result set */
		recordWindowStreamResultSet(10);
		replayAll();
		List<ResultSet> results = _resultStore.getResultsFromIndex(0);
		assertEquals(10, results.size());
		verifyAll();
	}
		
	@Test
	public void testGetResultsFromIndex_streamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(6);
		replayAll();
		List<ResultSet> resultSets = _resultStore.getResultsFromIndex(4);
		assertEquals(1, resultSets.size());
		ResultSet result = resultSets.get(0);
		result.last();
		assertEquals(6, result.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromIndex_streamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(6);
		replayAll();
		List<ResultSet> results = _resultStore.getResultsFromIndex(4);
		assertEquals(6, results.size());
		verifyAll();
	}
		
	
	@Test(expected=SNEEException.class)
	public void testGetResultFromIndexSingleton()
	throws SNEEException, SNEEConfigurationException {
		ResultStoreImpl singletonResultSet = 
			new ResultStoreImpl(testQuery, mockQEP) {
			
			protected ResultSetMetaData createMetaData(
					QueryExecutionPlan queryPlan)
			throws SQLException {
				return mockMetaData;
			}
			
			protected CircularArray<Output> createDataStore() {
				CircularArray<Output> dataList = 
					new CircularArray<Output>(1);
				dataList.add(mockOutput);//1
				return dataList;
			}
		}; 
		singletonResultSet.setCommand("SELECT * FROM TestStream;");
		singletonResultSet.getResultsFromIndex(1);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_negativeIndex() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(-4, 3);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_invalidIndex() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(400, 5);
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_invalidCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(2, 42);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_zeroCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(7, 0);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_negativeCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a negative number of results should throw exception */
		_resultStore.getResultsFromIndex(3, -42);
	}

	@Test
	public void testGetResultsFromIndexCount_zeroIndexStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(7);
		replayAll();
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromIndex(0, 7);
		assertEquals(1, resultSets.size());
		ResultSet result = resultSets.get(0);
		result.last();
		assertEquals(7, result.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromIndexCount_zeroIndexStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(7);
		replayAll();
		List<ResultSet> results = 
			_resultStore.getResultsFromIndex(0, 7);
		assertEquals(7, results.size());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexCount_invalidCountIndex() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(8, 5);
	}

	@Test
	public void testGetResultsFromIndexCountStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(2);
		replayAll();
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromIndex(4, 2);
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(2, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromIndexCountStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(2);
		replayAll();
		List<ResultSet> results = _resultStore.getResultsFromIndex(4, 2);
		assertEquals(2, results.size());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexJustLargerCountStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		replayAll();
		_resultStore.getResultsFromIndex(0, 11);
		verifyAll();
	}

	@Test
	public void testGetResultsFromIndexFullCountStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(10);
		replayAll();
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromIndex(0, 10);
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(10, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromIndexFullCountStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(6);
		replayAll();
		List<ResultSet> results = _resultStore.getResultsFromIndex(4, 6);
		assertEquals(6, results.size());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromIndexDuration() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		_resultStore.getResultsFromIndex(3, new Duration(3000));
	}

	@Ignore@Test
	public void testGetResultsFromIndex_duration() 
	throws SNEEException {
		//FIXME: Implement testGetResultsFromIndex_duration tests
		fail();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestamp_invalidFutureTimestamp() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 500000);
		_resultStore.getResultsFromTimestamp(ts);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestamp_invalidOldTimestamp() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/*
		 * Record 10 second result set
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 500000);
		_resultStore.getResultsFromTimestamp(ts);
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestamp_streamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(5);
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
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromTimestamp(ts);
		assertEquals(1, resultSets.size());
		ResultSet result = resultSets.get(0);
		result.last();
		assertEquals(5, result.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestamp_streamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(5);
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
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 5000);
		List<ResultSet> results = 
			_resultStore.getResultsFromTimestamp(ts);
		assertEquals(5, results.size());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_invalidFutureTimestamp() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 500000);
		_resultStore.getResultsFromTimestamp(ts, 3);
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_invalidOldTimestamp() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/*
		 * Record 10 second result set 
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 500000);
		_resultStore.getResultsFromTimestamp(ts, 3);
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_invalidCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		long currentTime = System.currentTimeMillis();
		Timestamp ts = new Timestamp(currentTime + 5000);
		_resultStore.getResultsFromTimestamp(ts, 49292);
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_negativeCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		_resultStore.getResultsFromTimestamp(ts, -92);
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampCount_zeroCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		_resultStore.getResultsFromTimestamp(ts, 0);
	}
	
	@Test
	public void testGetResultsFromTimestampCount_streamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(3);
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
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromTimestamp(ts, 3);
		assertEquals(1, resultSets.size());
		ResultSet result = resultSets.get(0);
		result.last();
		assertEquals(3, result.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestampCount_streamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(3);
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
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 5000);
		List<ResultSet> results = 
			_resultStore.getResultsFromTimestamp(ts, 3);
		assertEquals(3, results.size());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestampCount_fullSetStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(10);
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
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromTimestamp(ts, 10);
		assertEquals(1, resultSets.size());
		ResultSet result = resultSets.get(0);
		result.last();
		assertEquals(10, result.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestampCount_fullSetStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(10);
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
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 10000);
		List<ResultSet> results = 
			_resultStore.getResultsFromTimestamp(ts, 10);
		assertEquals(10, results.size());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_invalidFutureTimestamp() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 500000);
		_resultStore.getResultsFromTimestamp(ts, new Duration(45000));
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_invalidOldTimestamp() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/*
		 * Record 10 second result set
		 */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000);
		//Test
		replayAll();
		Timestamp ts = new Timestamp(System.currentTimeMillis() - 500000);
		_resultStore.getResultsFromTimestamp(ts, new Duration(30000));
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_zeroDuration() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		/* Request a 0 duration of results should throw exception */
		_resultStore.getResultsFromTimestamp(ts, new Duration(0));
	}

	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_negativeDuration() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		/* Requesting a negative duration should throw exception */
		_resultStore.getResultsFromTimestamp(ts, new Duration(-42));
	}
	
	@Test(expected=SNEEException.class)
	public void testGetResultsFromTimestampDuration_invalidDuraction() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		//Record result set of 10 seconds
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 10000).times(1).andReturn(currentTime);
		//Test
		replayAll();
		/* Request a larger duration of results than exist 
		 * should throw exception */
		Timestamp ts = new Timestamp(System.currentTimeMillis() + 5000);
		_resultStore.getResultsFromTimestamp(ts, new Duration(42, TimeUnit.DAYS));
	}
	
	@Test
	public void testGetResultsFromTimestampDuration_validSubDuractionStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(2);
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
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromTimestamp(ts, 
				new Duration(2, TimeUnit.SECONDS));
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(2, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetResultsFromTimestampDuration_validSubDuractionStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(2);
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
		replayAll();
		Timestamp ts = new Timestamp(currentTime - 5000);
		List<ResultSet> resultSets = 
			_resultStore.getResultsFromTimestamp(ts, 
				new Duration(2, TimeUnit.SECONDS));
		assertEquals(2, resultSets.size());
		verifyAll();
	}
	
	@Test
	public void testGetNewestResults_fullCountStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(10);
		replayAll();
		/* Request the full set back */
		List<ResultSet> resultSets = _resultStore.getNewestResults(10);
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(10, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetNewestResults_fullCountStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(10);
		replayAll();
		/* Request the full set back */
		List<ResultSet> results = _resultStore.getNewestResults(10);
		assertEquals(10, results.size());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetNewestResults_invalidCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a larger number of results than exist 
		 * should throw exception */
		_resultStore.getNewestResults(45);
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_zeroCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a 0 number of results should throw exception */
		_resultStore.getNewestResults(0);
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_negativeCount() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a negative number of results should throw exception */
		_resultStore.getNewestResults(-42);
	}
	
	@Test
	public void testGetNewestResults_countStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		/*
		 * Test resultset contains 10 tuples
		 * Answer set should only contain the most recent 2
		 */
		int count = 2;
		recordTupleStreamResultSet(count);
		replayAll();
		List<ResultSet> resultSets = 
			_resultStore.getNewestResults(count);
		assertEquals(1, resultSets.size());
		ResultSet results = resultSets.get(0);
		results.last();
		assertEquals(count, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetNewestResults_countStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		/*
		 * Test resultset contains 10 tuples
		 * Answer set should only contain the most recent 2
		 */
		int count = 2;
		recordWindowStreamResultSet(count);
		replayAll();
		List<ResultSet> results = _resultStore.getNewestResults(count);
		assertEquals(count, results.size());
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetNewestResults_zeroDuration() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a 0 duration of results should throw exception */
		_resultStore.getNewestResults(new Duration(0));
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_negativeDuration() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Requesting a negative duration should throw exception */
		_resultStore.getNewestResults(new Duration(-42));
	}

	@Test(expected=SNEEException.class)
	public void testGetNewestResults_invalidDuraction() 
	throws SNEEException, SNEEConfigurationException {
		setUpTupleStream();
		/* Request a larger duration of results than exist 
		 * should throw exception */
		long currentTime = System.currentTimeMillis();
		expect(mockOutput.getEvalTime()).andReturn(currentTime - 300000);
		replayAll();
		_resultStore.getNewestResults(new Duration(42, TimeUnit.DAYS));
		verifyAll();
	}
	
	@Test
	public void testGetNewestResults_validSubDuractionStreamTuples() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpTupleStream();
		recordTupleStreamResultSet(5);
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
		List<ResultSet> resultSets = 
			_resultStore.getNewestResults(
					new Duration(5, TimeUnit.SECONDS));
		assertEquals(1, resultSets.size());
		ResultSet results = 
			resultSets.get(0);
		results.last();
		assertEquals(5, results.getRow());
		verifyAll();
	}
	
	@Test
	public void testGetNewestResults_validSubDuractionStreamWindows() 
	throws SNEEException, SQLException, SNEEConfigurationException {
		setUpWindowStream();
		recordWindowStreamResultSet(5);
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
		replayAll();
		List<ResultSet> results = 
			_resultStore.getNewestResults(
					new Duration(5, TimeUnit.SECONDS));
		assertEquals(5, results.size());
		verifyAll();
	}
	
}
