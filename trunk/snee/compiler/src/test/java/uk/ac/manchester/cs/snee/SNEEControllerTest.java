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
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.MetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.translator.ParserValidationException;
import uk.ac.manchester.cs.snee.data.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.evaluator.Dispatcher;
import uk.ac.manchester.cs.snee.evaluator.EvaluatorException;
import uk.ac.manchester.cs.snee.evaluator.StreamResultSet;
import antlr.RecognitionException;
import antlr.TokenStreamException;

public class SNEEControllerTest extends EasyMockSupport {

	private SNEEController _snee;
	private String mQuery = "SELECT * FROM HerneBay_Tide;";
	
	// Create mock objects
	final Metadata mockSchema = createMock(Metadata.class);
	final QueryCompiler mockQueryCompiler = createMock(QueryCompiler.class);
	final Dispatcher mockDispatcher = createMock(Dispatcher.class);
	final LAF mockPlan = createMock(LAF.class);
	final StreamResultSet mockResultset = createMock(StreamResultSet.class);
		
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
			
			protected Metadata initialiseSchema() 
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
			
			protected StreamResultSet createStreamResultSet() {
//				System.out.println("Overridden createStreamResultSet()");
				return mockResultset;
			}
		};
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected=SNEEException.class)
	public void testAddQuery_NullParams() 
	throws SNEEException, SchemaMetadataException, EvaluatorException 
	{
		_snee.addQuery(null);	
	}

	@Test(expected=SNEEException.class)
	public void testAddQuery_EmptyQuery() 
	throws SNEEException, SchemaMetadataException, EvaluatorException 
	{
		_snee.addQuery("");	
	}

	@Test(expected=SNEEException.class)
	public void testAddQuery_WhitespaceQuery() 
	throws SNEEException, SchemaMetadataException, EvaluatorException 
	{
		_snee.addQuery("   ");	
	}
	
	@Test
	public void testAddQuery_Valid() 
	throws SNEEException, SchemaMetadataException, 
	SourceDoesNotExistException, TypeMappingException, 
	ParserValidationException, OptimizationException, ParserException, 
	EvaluatorException 
	{
		assertEquals(1, _snee.addQuery(mQuery));
	}

	@Test(expected=SNEEException.class)
	public void testRemoveQuery_invalidQuery() 
	throws SNEEException {
		//Record expected calls to mock objects
		expect(mockDispatcher.stopQuery(36)).andThrow(new SNEEException("Exception expected"));
		
		//Test
		replayAll();
		_snee.removeQuery(36);
		verifyAll();
	}
	
	@Test
	public void testRemoveQuery_queryAdded() 
	throws SNEEException, SourceDoesNotExistException, 
	TypeMappingException, SchemaMetadataException, 
	ParserValidationException, OptimizationException, ParserException, 
	ExtentDoesNotExistException, EvaluatorException,
	RecognitionException, TokenStreamException, SNEEConfigurationException {
		//Record expected calls to the mock objects
		expect(mockQueryCompiler.compileQuery(1, mQuery)).andReturn(mockPlan);
		mockDispatcher.startQuery(1, mockResultset, mockPlan);
		expect(mockDispatcher.stopQuery(1)).andReturn(true);
		
		//Test
		replayAll();
		int qid = _snee.addQuery(mQuery);
		_snee.removeQuery(qid);
		verifyAll();
	}

	@Test
	public void testClose() 
	throws SourceDoesNotExistException, SNEEException, 
	TypeMappingException, SchemaMetadataException, 
	ParserValidationException, OptimizationException, ParserException, 
	ExtentDoesNotExistException, EvaluatorException,
	RecognitionException, TokenStreamException, SNEEConfigurationException {
		//Record expected calls to the mock objects
		expect(mockQueryCompiler.compileQuery(1, mQuery)).andReturn(mockPlan);
		mockDispatcher.startQuery(1, mockResultset, mockPlan);
		expect(mockQueryCompiler.compileQuery(2, mQuery)).andReturn(mockPlan);
		mockDispatcher.startQuery(2, mockResultset, mockPlan);
		mockDispatcher.close();
		
		//Test
		replayAll();
		_snee.addQuery(mQuery);
		_snee.addQuery(mQuery);
		_snee.close();
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_invalidQueryId_noQueries() 
	throws SNEEException {
		_snee.getResultSet(49);
	}

	@Test(expected=SNEEException.class)
	public void testGetResults_invalidQueryId() 
	throws SourceDoesNotExistException, SNEEException, 
	TypeMappingException, SchemaMetadataException, 
	ParserValidationException, OptimizationException, ParserException,
	EvaluatorException  {
		int qID = _snee.addQuery(mQuery);
		_snee.getResultSet(qID*20);
	}

	@Test
	public void testGetResults_validQueryId() 
	throws SNEEException, SchemaMetadataException, 
	SourceDoesNotExistException, TypeMappingException, 
	ParserValidationException, OptimizationException, ParserException, 
	ExtentDoesNotExistException, EvaluatorException,
	RecognitionException, TokenStreamException, SNEEConfigurationException {
		//Record expected calls to the mock objects
		expect(mockQueryCompiler.compileQuery(1, mQuery)).andReturn(mockPlan);
		mockDispatcher.startQuery(1, mockResultset, mockPlan);
		
		//Test
		replayAll();		
		int qID = _snee.addQuery(mQuery);
		StreamResultSet result = _snee.getResultSet(qID);
		assertNotNull(result);
		verifyAll();
	}
	
	@Test(expected=MalformedURLException.class)
	public void testAddServiceSource_invalidURL() 
	throws MalformedURLException, SchemaMetadataException, 
	TypeMappingException, SNEEDataSourceException 
	{
		//Record responses
		String testUrl = "not a url";
		//Have to use expectLastCall method since method had void return type 
		mockSchema.addWebServiceSource(testUrl);
		expectLastCall().andThrow(new MalformedURLException());
		//Test
		replayAll();
		_snee.addServiceSource(testUrl);
		verifyAll();
	}

	@Test
	public void testAddServiceSource_validURL() 
	throws MalformedURLException, SchemaMetadataException, 
	TypeMappingException, SNEEDataSourceException 
	{
		//FIXME: Setup expected answers from mock objects
		String url = 
			"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl";
		_snee.addServiceSource(url);
	}
	
}
