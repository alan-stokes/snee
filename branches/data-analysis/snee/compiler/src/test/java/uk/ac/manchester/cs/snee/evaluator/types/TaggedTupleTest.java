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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TaggedTupleTest extends EasyMockSupport {

	DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	DateFormat tf = new SimpleDateFormat("hh:mm:ss");
	DateFormat dtf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				TaggedTupleTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	private TaggedTuple taggedTuple;
	private long testSetupTime;
	private Tuple mockTuple;

	@Before
	public void setUp() throws Exception {
		mockTuple = createMock(Tuple.class); 
		testSetupTime = System.currentTimeMillis();
		taggedTuple = new TaggedTuple(mockTuple);
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testGetEvalTime() {
		assertTrue(taggedTuple.getEvalTime() >= testSetupTime);
		assertTrue(taggedTuple.getEvalTime() <= 
			System.currentTimeMillis());
	}
	
	@Test
	public void testGetIndex_indexNotSet() {
		assertEquals(-1, taggedTuple.getIndex());
	}
	
	@Test
	public void testGetIndex() {
		taggedTuple.setIndex(302);
		assertEquals(302, taggedTuple.getIndex());
	}

	@Test
	public void testGetTuple() {
		assertTrue(taggedTuple.getTuple() instanceof Tuple);
		assertEquals(mockTuple, taggedTuple.getTuple());
	}
	
}
