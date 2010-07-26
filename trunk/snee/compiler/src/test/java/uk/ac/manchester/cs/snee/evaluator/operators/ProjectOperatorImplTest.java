package uk.ac.manchester.cs.snee.evaluator.operators;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.Operator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.ProjectOperator;
import uk.ac.manchester.cs.snee.evaluator.EndOfResultsException;
import uk.ac.manchester.cs.snee.evaluator.types.Field;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.ReceiveTimeoutException;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;

public class ProjectOperatorImplTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				ProjectOperatorImplTest.class.getResource(
						"etc/log4j.properties"));
	}

	final Expression mockExpression = createMock(Expression.class);
	final Tuple mockTuple = createMock(Tuple.class);
	final ProjectOperator mockProjOp = createMock(ProjectOperator.class);
	final List<Attribute> mockQPAttributesList = createMock(List.class);
	final Attribute mockAttr = createMock(Attribute.class);
	final Field mockField = createMock(Field.class);
//	final List<Attribute> mockAttrList = createMock(List.class);
	
	@Before
	public void setUp() throws Exception {
		//Record create project operator behaviour
		expect(mockProjOp.getAttributes()).andReturn(mockQPAttributesList);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected=SNEEException.class)
	public void testEvaluateSingleExpression_invalidField() 
	throws SNEEException, SchemaMetadataException {
		/*
		 * Record behaviour
		 */
		expect(mockExpression.getRequiredAttributes()).andReturn(mockQPAttributesList);
		expect(mockQPAttributesList.get(0)).andReturn(mockAttr);
		expect(mockAttr.getAttributeName()).andReturn("attrName");
		expect(mockAttr.getLocalName()).andReturn("extent");
		expect(mockTuple.getField("extent.attrName")).andThrow(new SNEEException("Non existent field"));
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		op.evaluateSingleExpression(mockExpression, mockTuple);
		verifyAll();
	}

	@Test
	public void testEvaluateSingleExpression_validField() 
	throws SNEEException, SchemaMetadataException {
		// Record behaviour
		expect(mockExpression.getRequiredAttributes()).andReturn(mockQPAttributesList);
		expect(mockQPAttributesList.get(0)).andReturn(mockAttr);
		expect(mockAttr.getAttributeName()).andReturn("attrName");
		expect(mockAttr.getLocalName()).andReturn("extent");
		expect(mockTuple.getField("extent.attrName")).andReturn(mockField);
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		Field field = op.evaluateSingleExpression(mockExpression, mockTuple);
		assertEquals(mockField, field);
		verifyAll();
	}

	@Ignore@Test
	public void testEvaluateMultiExpression() {
		fail("Not yet implemented");
	}

	@Test
	public void testEvaluate_floatIntMult() {
		//Record
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		Object answer = op.evaluate(new Float(5.28), 9, MultiType.MULTIPLY);
		assertEquals(47.52, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}

	@Test
	public void testEvaluate_bigDecIntMult() {
		//Record
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		Object answer = op.evaluate(new BigDecimal(0.0000), 9, MultiType.MULTIPLY);
		assertEquals(0.0000, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}
	
	@Test
	public void testEvaluate_intFloatMult() {
		//Record
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		Object answer = op.evaluate(9, new Float(0.13), MultiType.MULTIPLY);
		assertEquals(1.17, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}
//
//	@Test
//	public void testGetNext() 
//	throws ReceiveTimeoutException, SNEEException, EndOfResultsException 
//	{
//		//Record
//		final ReceiveOperatorImpl mockChildOp = createMock(ReceiveOperatorImpl.class);
//		Collection<Output> tupleList = new ArrayList<Output>();
//		Tuple tuple = new Tuple();
//		tupleList.add(new TaggedTuple(tuple));
//		expect(mockChildOp.getNext()).andReturn(tupleList);
//		//Test
//		replayAll();
//		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp) {
//			public EvaluatorOperator getEvaluatorOperator(Operator op) 
//			throws SNEEException, SchemaMetadataException {
//				System.out.println("created mock child");
//				return mockChildOp;
//			}
//		};
//		op.getNext();
//		verifyAll();
//	}

}
