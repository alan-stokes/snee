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
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.MalformedURLException;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.QueryCompiler;
import uk.ac.manchester.cs.snee.compiler.allocator.SourceAllocatorException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSException;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlanAbstract;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.ExpressionException;
import uk.ac.manchester.cs.snee.compiler.sn.router.RouterException;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.evaluator.Dispatcher;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import antlr.RecognitionException;
import antlr.TokenStreamException;

public class SNEEControllerTest extends EasyMockSupport {

	private SNEEController _snee;
	private String mQuery = "SELECT * FROM HerneBay_Tide;";

	// Create mock objects
	final MetadataManager mockSchema = createMock(MetadataManager.class);
	final QueryCompiler mockQueryCompiler = createMock(QueryCompiler.class);
	final Dispatcher mockDispatcher = createMock(Dispatcher.class);
	final QueryExecutionPlanAbstract mockPlan = createMock(QueryExecutionPlanAbstract.class);
	final ResultStore mockResultset = createMock(ResultStore.class);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				SNEEControllerTest.class.getClassLoader().getResource(
				"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		_snee = new SNEEController("etc/snee.properties") {

			protected MetadataManager initialiseMetadata() 
			throws TypeMappingException, SchemaMetadataException, 
			MetadataException, UnsupportedAttributeTypeException {
				//				System.out.println("Overridden initialiseSchemaMetadata()");
				return mockSchema;
			}

			protected QueryCompiler initialiseQueryCompiler() {
				//				System.out.println("Overridden initialiseQueryCompiler()");
				return mockQueryCompiler;
			}

			protected Dispatcher initialiseDispatcher() {
				//				System.out.println("Overridden initialiseDispatcher()");
				return mockDispatcher;
			}

			protected ResultStore createStreamResultSet(String query, QueryExecutionPlan mockPlan) {
				//				System.out.println("Overridden createStreamResultSet()");
				return mockResultset;
			}
		};
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected=SNEECompilerException.class)
	public void testAddQuery_NullParams()
	throws SchemaMetadataException, EvaluatorException, 
	SNEECompilerException, SNEEException, MetadataException,
	SNEEConfigurationException 
	{
		_snee.addQuery(null, null);	
	}

	@Test(expected=SNEECompilerException.class)
	public void testAddQuery_EmptyQuery() 
	throws SchemaMetadataException, EvaluatorException, 
	SNEECompilerException, SNEEException, MetadataException,
	SNEEConfigurationException 
	{
		_snee.addQuery("", null);	
	}

	@Test(expected=SNEECompilerException.class)
	public void testAddQuery_WhitespaceQuery() 
	throws SchemaMetadataException, EvaluatorException, 
	SNEECompilerException, SNEEException, MetadataException,
	SNEEConfigurationException 
	{
		_snee.addQuery("   ", null);	
	}

	@Test
	public void testAddQuery_Valid() 
	throws SchemaMetadataException, EvaluatorException, 
	SNEECompilerException, SNEEException, MetadataException,
	RecognitionException, TokenStreamException, TypeMappingException, 
	ExpressionException, OptimizationException, ParserException,
	SNEEConfigurationException 
	{
		assertEquals(1, _snee.addQuery(mQuery, 
			"src/test/resources/etc/query-parameters.xml"));
	}

	@Test(expected=SNEEException.class)
	public void testRemoveQuery_invalidQuery() 
	throws SNEEException {
		//Record expected calls to mock objects
		expect(mockDispatcher.stopQuery(36)).andThrow(
				new SNEEException("Exception expected"));

		//Test
		replayAll();
		_snee.removeQuery(36);
		verifyAll();
	}

	@Test
	public void testRemoveQuery_queryAdded() 
	throws RecognitionException, TokenStreamException, SNEEException, 
	TypeMappingException, SchemaMetadataException,
	ExpressionException, OptimizationException, ParserException, 
	SNEEConfigurationException, MetadataException, EvaluatorException,
	SNEECompilerException, SourceAllocatorException, WhenSchedulerException, RouterException  
	{		//Record expected calls to the mock objects
		_snee.resetQueryId();
		expect(mockQueryCompiler.compileQuery(1, mQuery, null)).andReturn(mockPlan);
		mockDispatcher.startQuery(1, mockResultset, mockPlan);
		mockDispatcher.giveAutonomicManagerQuery(mQuery);
		expect(mockDispatcher.stopQuery(1)).andReturn(true);

		//Test
		replayAll();
		int qid = _snee.addQuery(mQuery, null);
		_snee.removeQuery(qid);
		verifyAll();
	}

	@Test
	public void testClose() 
	throws SNEEException, 
	TypeMappingException, SchemaMetadataException, 
	ExpressionException, OptimizationException, ParserException, 
	EvaluatorException, RecognitionException, TokenStreamException,
	SNEEConfigurationException, SNEECompilerException, MetadataException, 
	SourceAllocatorException, WhenSchedulerException, RouterException {
		//Record expected calls to the mock objects
		_snee.resetQueryId();
		expect(mockQueryCompiler.compileQuery(1, mQuery, null)).andReturn(mockPlan);
		mockDispatcher.startQuery(1, mockResultset, mockPlan);
		mockDispatcher.giveAutonomicManagerQuery(mQuery);
		expect(mockQueryCompiler.compileQuery(2, mQuery, null)).andReturn(mockPlan);
		mockDispatcher.startQuery(2, mockResultset, mockPlan);
		mockDispatcher.giveAutonomicManagerQuery(mQuery);
		mockDispatcher.close();

		//Test
		replayAll();
		_snee.addQuery(mQuery, null);
		_snee.addQuery(mQuery, null);
		_snee.close();
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_invalidQueryId_noQueries() 
	throws SNEEException {
		_snee.getResultStore(49);
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_invalidQueryId() 
	throws SNEEException, 
	TypeMappingException, SchemaMetadataException, 
	ExpressionException, OptimizationException, ParserException,
	EvaluatorException, QoSException, SNEECompilerException,
	MetadataException, SNEEConfigurationException  {
		int qID = _snee.addQuery(mQuery, null);
		_snee.getResultStore(qID*20);
	}

	@Test
	public void testGetResults_validQueryId() 
	throws SNEEException, SchemaMetadataException, 
	TypeMappingException, 
	ExpressionException, OptimizationException, ParserException, 
	EvaluatorException, RecognitionException, TokenStreamException, 
	SNEEConfigurationException, SNEECompilerException, MetadataException,
	SourceAllocatorException, WhenSchedulerException, RouterException
	{
		//Record expected calls to the mock objects
		_snee.resetQueryId();
		expect(mockQueryCompiler.compileQuery(1, mQuery, null)).andReturn(mockPlan);
		mockDispatcher.startQuery(1, mockResultset, mockPlan);
		mockDispatcher.giveAutonomicManagerQuery(mQuery);
		
		//Test
		replayAll();		
		int qID = _snee.addQuery(mQuery, null);
		ResultStore result = _snee.getResultStore(qID);
		assertNotNull(result);
		verifyAll();
	}

	@Test(expected=MalformedURLException.class)
	public void testAddServiceSource_invalidURL() 
	throws MalformedURLException, SchemaMetadataException, 
	TypeMappingException, SNEEDataSourceException, 
	SourceMetadataException, MetadataException 
	{
		//Record responses
		String testUrl = "not a url";
		//Have to use expectLastCall method since method had void return type 
		mockSchema.addDataSource("bad url", testUrl, 
				SourceType.PULL_STREAM_SERVICE);
		expectLastCall().andThrow(new MalformedURLException());
		//Test
		replayAll();
		_snee.addServiceSource("bad url", testUrl,
				SourceType.PULL_STREAM_SERVICE);
		verifyAll();
	}

	@Test
	public void testAddServiceSource_validURL() 
	throws MalformedURLException, SchemaMetadataException, 
	TypeMappingException, SNEEDataSourceException,
	SourceMetadataException, MetadataException 
	{
		String url = 
			"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl";
		mockSchema.addDataSource("CCO-WS", url, 
				SourceType.PULL_STREAM_SERVICE);
		replayAll();
		_snee.addServiceSource("CCO-WS", url, 
				SourceType.PULL_STREAM_SERVICE);
		verifyAll();
	}

}
