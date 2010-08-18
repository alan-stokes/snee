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
package uk.ac.manchester.cs.snee.evaluator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.StreamResultSet;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;

public class DispatcherTest extends EasyMockSupport {
	
	//Mock objects
	final Metadata mockSchema = createMock(Metadata.class);
	
	private Dispatcher _dispatcher;
	MockQueryEvaluator mockEvaluator = 
		new MockQueryEvaluator(1, null, null);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				DispatcherTest.class.getClassLoader().getResource(
						"etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		_dispatcher = new Dispatcher(mockSchema) {
			protected QueryEvaluator createQueryEvaluator(int queryId, 
					LAF queryPlan, StreamResultSet resultSet) {
				System.out.println("Overridden createQueryEvaluator()");
				mockEvaluator._queryId = queryId;
				return mockEvaluator;
			}
		};
	}

	@After
	public void tearDown() throws Exception {
		_dispatcher.close();
	}
	
	@Test
	public void testRunQuery_mock() throws SNEEException {
		_dispatcher.close();
		mockEvaluator.validateFinishedExecuting();
	}
	
	@Test@Ignore
	public void testStopQuery_validID() 
	throws SNEEException, SchemaMetadataException, 
	EvaluatorException, MetadataException {
		_dispatcher.startQuery(1, null, null);
//		System.out.println("Number of running queries: " + _evaluatorController.getRunningQueryIds().size());
		try {
			assertTrue(_dispatcher.stopQuery(1));
			mockEvaluator.validateFinishedExecuting();
		} catch (SNEEException e) {
			fail("Unexpected exception. " + e);
		}
	}
	
	@Test(expected=SNEEException.class)
	public void testStopQuery_invalidID() throws SNEEException {
		_dispatcher.stopQuery(42);
	}
	
	@Test
	public void testClose_noqueries() {
		_dispatcher.close();
	}

	@Test@Ignore
	public void testClose() 
	throws SNEEException, SchemaMetadataException, EvaluatorException,
	MetadataException {
		_dispatcher.startQuery(1, null, null);
		_dispatcher.startQuery(2, null, null);
		_dispatcher.startQuery(3, null, null);
		_dispatcher.close();
		
		try {
			_dispatcher.stopQuery(1);
			fail("Query should already have stopped!");
		} catch (SNEEException e1) {
			//Expected exception
			try {
				_dispatcher.stopQuery(2);
				fail("Query should already have stopped!");
			} catch (SNEEException e2) {
				//Expected exception
				try {
					_dispatcher.stopQuery(3);
					fail("Query should already have stopped!");
				} catch (SNEEException e3) {
					//Expected exception
				}
			}
		}
	}
	
	private class MockQueryEvaluator extends QueryEvaluator {

		private int _queryId;
		private boolean isExecuting = false;
		
		protected MockQueryEvaluator(int queryId, LAF queryPlan, ExtentMetadata schema) {
//			System.out.println("MockQueryEvaluator constructor");
			_queryId = queryId;
		}
		
		public void run() {
//			System.out.println("MockEvaluator running");
			isExecuting = true;
		}
		
		public void stopExecuting() {
//			System.out.println("Stopping MockEvaluator");
			isExecuting = false;
		}
		
		public int getQueryId() {
			return _queryId;
		}
		
		public void validateFinishedExecuting() {
			assertFalse(isExecuting);
		}
	}
}
