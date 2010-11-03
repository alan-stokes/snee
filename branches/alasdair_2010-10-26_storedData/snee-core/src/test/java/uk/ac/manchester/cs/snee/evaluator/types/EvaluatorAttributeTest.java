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
package uk.ac.manchester.cs.snee.evaluator.types;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

public class EvaluatorAttributeTest extends EasyMockSupport {
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				EvaluatorAttributeTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private Attribute mockAttribute =
		createMock(Attribute.class);
	private AttributeType mockType =
		createMock(AttributeType.class);
	private Object mockData = 
		createMock(Object.class);

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected=SchemaMetadataException.class)
	public void testGetData_unsupportedDataType() 
	throws SchemaMetadataException {
		expect(mockAttribute.getExtentName()).
			andReturn("streamName");
		expect(mockAttribute.getAttributeSchemaName()).
			andReturn("attrName");
		expect(mockAttribute.getAttributeDisplayName()).
			andReturn("streamName.attrName");
		expect(mockAttribute.getType()).andReturn(mockType);
		expect(mockType.getName()).andReturn("attrType");
		replayAll();
		EvaluatorAttribute attr = 
			new EvaluatorAttribute(mockAttribute, mockData);
		assertEquals(mockData, attr.getData());
		verifyAll();
	}

	@Test
	public void testGetData() 
	throws SchemaMetadataException {
		expect(mockAttribute.getExtentName()).
			andReturn("streamName");
		expect(mockAttribute.getAttributeSchemaName()).
			andReturn("attrName");
		expect(mockAttribute.getAttributeDisplayName()).
			andReturn("streamName.attrName");
		expect(mockAttribute.getType()).andReturn(mockType);
		expect(mockType.getName()).andReturn("integer");
		replayAll();
		EvaluatorAttribute attr = 
				new EvaluatorAttribute(mockAttribute, mockData);
		assertEquals(mockData, attr.getData());
		verifyAll();
	}

}
