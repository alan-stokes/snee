package uk.ac.manchester.cs.snee.operators.evaluator;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.easymock.classextension.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;

public class ProjectOperatorImplTest extends EasyMockSupport {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Configure logging
		PropertyConfigurator.configure(
				ProjectOperatorImplTest.class.getClassLoader().
				getResource("etc/log4j.properties"));
	}

	final Expression mockExpression = 
		createMock(Expression.class);
	final MultiExpression mockMultiExpr =
		createMock(MultiExpression.class);
	final Tuple mockTuple = createMock(Tuple.class);
	final ProjectOperator mockProjOp = 
		createMock(ProjectOperator.class);
	final List<Attribute> mockQPAttributesList = 
		createMock(List.class);
	final Attribute mockAttr = createMock(Attribute.class);
	final EvaluatorAttribute mockField = 
		createMock(EvaluatorAttribute.class);
//	final List<Attribute> mockAttrList = createMock(List.class);
	
	@Before
	public void setUp() throws Exception {
		//Record create project operator behaviour
		expect(mockProjOp.getAttributes()).
			andReturn(mockQPAttributesList);
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
		expect(mockExpression.getRequiredAttributes()).
			andReturn(mockQPAttributesList);
		expect(mockQPAttributesList.get(0)).andReturn(mockAttr);
		expect(mockAttr.getAttributeSchemaName()).
			andReturn("attrName");
		expect(mockAttr.getExtentName()).andReturn("extent");
		expect(mockTuple.getAttribute("extent", "attrName")).
			andThrow(new SNEEException("Non existent field"));
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
		expect(mockExpression.getRequiredAttributes()).
			andReturn(mockQPAttributesList);
		expect(mockQPAttributesList.get(0)).andReturn(mockAttr);
		expect(mockAttr.getAttributeSchemaName()).
			andReturn("attrName");
		expect(mockAttr.getExtentName()).andReturn("extent");
		expect(mockTuple.getAttribute("extent", "attrName")).
			andReturn(mockField);
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		EvaluatorAttribute attr = 
			op.evaluateSingleExpression(mockExpression, mockTuple);
		assertEquals(mockField, attr);
		verifyAll();
	}

	@Test
	public void testEvaluateMultiExpression() 
	throws SNEEException, SchemaMetadataException,
	TypeMappingException {
		/*
		 * Test 9 * attr
		 */
		DataAttribute mockDA = createMock(DataAttribute.class);
		IntLiteral mockInt = createMock(IntLiteral.class);
		List<String> mockAttrNames = createMock(List.class);
		AttributeType mockAttrType = createMock(AttributeType.class);
		
		// Record behaviour
		expect(mockMultiExpr.getExpressions()).
			andReturn(new Expression[]{mockDA, mockInt});
		expect(mockDA.getExtentName()).andReturn("extent");
		expect(mockDA.getAttributeSchemaName()).andReturn("attr");
		expect(mockDA.getAttributeDisplayName()).
			andReturn("extent.attr").times(2);
		expect(mockTuple.getAttributeValueByDisplayName("extent.attr")).
			andReturn(21);
		expect(mockInt.getValue()).andReturn(9).times(1,2);
		expect(mockMultiExpr.getMultiType()).
			andReturn(MultiType.MULTIPLY);
		expect(mockMultiExpr.getType()).
			andReturn(mockAttrType).times(1, 2);
		expect(mockAttrType.getName()).
			andReturn("integer").times(2, 4);
		replayAll();
		
		// Run test
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		EvaluatorAttribute attr = 
			op.evaluateMultiExpression(mockMultiExpr, mockTuple);
		assertEquals(189, ((Double)attr.getData()).intValue());
		verifyAll();
	}

	@Test
	public void testEvaluate_floatIntMult() {
		//Record
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		Object answer = 
			op.evaluate(new Float(5.28), 9, MultiType.MULTIPLY);
		assertEquals(47.52, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}

	@Test
	public void testEvaluate_bigDecIntMult() {
		//Record
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		Object answer = 
			op.evaluate(new BigDecimal(0.0000), 9, MultiType.MULTIPLY);
		assertEquals(0.0000, ((Number) answer).doubleValue(), 0.1);
		verifyAll();
	}
	
	@Test
	public void testEvaluate_intFloatMult() {
		//Record
		//Test
		replayAll();
		ProjectOperatorImpl op = new ProjectOperatorImpl(mockProjOp);
		Object answer = 
			op.evaluate(9, new Float(0.13), MultiType.MULTIPLY);
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
