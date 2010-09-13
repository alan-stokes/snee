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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;

public class TupleTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				TupleTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private EvaluatorAttribute mockEvaluatorAttribute =
		createMock(EvaluatorAttribute.class);
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetAttributes_notSet() {
		Tuple tuple = new Tuple();
		assertEquals(true, tuple.getAttributeValues().isEmpty());
		assertEquals(0, tuple.size());
	}

	@Test
	public void testAddAttribute() 
	throws SNEEException, TypeMappingException, 
	SchemaMetadataException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name");
		replayAll();
		Tuple tuple = new Tuple();
		assertEquals(0, tuple.size());
		tuple.addAttribute(mockEvaluatorAttribute);
		assertEquals(1, tuple.size());
		verifyAll();
	}

	@Test
	public void testAddAttribute_alreadyExists() 
	throws SNEEException, TypeMappingException, 
	SchemaMetadataException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(2);
		replayAll();
		List<EvaluatorAttribute> attrs =
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		assertEquals(1, tuple.size());
		tuple.addAttribute(mockEvaluatorAttribute);
		assertEquals(2, tuple.size());
		verifyAll();
	}
	
	@Test
	public void testGetAttributeValues() {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		assertEquals(3, tuple.getAttributeValues().size());
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetAttrByIndex_notSet() 
	throws SNEEException {
		Tuple tuple = new Tuple();
		tuple.getAttribute(2);
	}

	@Test(expected=SNEEException.class)
	public void testGetAttributeByName_notSet() 
	throws SNEEException {
		Tuple tuple = new Tuple();
		tuple.getAttribute("field");
	}
	
	@Test(expected=SNEEException.class)
	public void testGetAttributeByIndex_notExists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttribute(60);
		verifyAll();
	}
	
	@Test(expected=SNEEException.class)
	public void testGetAttributeByName_notExists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttribute("field");
		verifyAll();
	}
	
	@Test
	public void testGetAttributeByIndex_exists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttribute(2);
		verifyAll();
	}
	
	@Test
	public void testGetAttributeByName_exists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttribute("name");
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetAttributeValueByIndex_notSet() 
	throws SNEEException {
		Tuple tuple = new Tuple();
		tuple.getAttributeValue(4);
	}

	@Test(expected=SNEEException.class)
	public void testGetAttributeValueByName_notSet() 
	throws SNEEException {
		Tuple tuple = new Tuple();
		tuple.getAttributeValue("name");
	}

	@Test(expected=SNEEException.class)
	public void testGetAttributeValueByIndex_notExists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttributeValue(3);
		verifyAll();
	}

	@Test(expected=SNEEException.class)
	public void testGetAttributeValueByName_notExists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttributeValue("field");
		verifyAll();
	}
	
	@Test
	public void testGetAttributeValueByIndex_exists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		expect(mockEvaluatorAttribute.getData()).andReturn(null);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttributeValue(0);
		verifyAll();
	}
	
	@Test
	public void testGetAttributeValueByName_exists() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		expect(mockEvaluatorAttribute.getData()).andReturn(null);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttributeValue("name");
		verifyAll();
	}
	
	@Test
	public void testGetAttributeValueByName_existsCaseInsensitive() 
	throws SNEEException {
		expect(mockEvaluatorAttribute.getAttributeSchemaName()).
			andReturn("name").times(3);
		expect(mockEvaluatorAttribute.getData()).andReturn(null);
		replayAll();
		List<EvaluatorAttribute> attrs = 
			new ArrayList<EvaluatorAttribute>();
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		attrs.add(mockEvaluatorAttribute);
		Tuple tuple = new Tuple(attrs);
		tuple.getAttributeValue("nAmE");
		verifyAll();
	}

}
