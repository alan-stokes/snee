package uk.ac.manchester.cs.snee.evaluator.operators;

import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.Operator;
import uk.ac.manchester.cs.snee.operators.logical.OperatorDataType;

public class MockOperator implements Operator {
		
		private String mOperatorName = "MOCKOPERATOR";

		public MockOperator() {
			System.out.println("ENTER MockOperator");
			System.out.println("EXIT MockOperator");
		}

		public boolean acceptsPredicates() {
			// TODO Auto-generated method stub
			return false;
		}

		public Iterator<Operator> childOperatorIterator() {
			// TODO Auto-generated method stub
			return null;
		}

		public List<Attribute> getAttributes() {
			// TODO Auto-generated method stub
			return null;
		}

		public int getCardinality(CardinalityType card) {
			// TODO Auto-generated method stub
			return 0;
		}

		public List<Expression> getExpressions() {
			// TODO Auto-generated method stub
			return null;
		}

		public Operator getInput(int index) {
			// TODO Auto-generated method stub
			return null;
		}

		public Node[] getInputs() {
			// TODO Auto-generated method stub
			return null;
		}

		public OperatorDataType getOperatorDataType() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getOperatorName() {
			// TODO Auto-generated method stub
			return null;
		}

		public Operator getOutput(int index) {
			// TODO Auto-generated method stub
			return null;
		}

		public String getParamStr() {
			// TODO Auto-generated method stub
			return null;
		}

		public Operator getParent() {
			// TODO Auto-generated method stub
			return null;
		}

		public Expression getPredicate() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getText() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getTupleAttributesStr(int maxPerLine)
				throws SchemaMetadataException, TypeMappingException {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean isAttributeSensitive() {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean isLocationSensitive() {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean isRecursive() {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean isRemoveable() {
			// TODO Auto-generated method stub
			return false;
		}

		public void pushLocalNameDown(String newLocalName) {
			// TODO Auto-generated method stub
			
		}

		public boolean pushProjectionDown(
				List<Expression> projectExpressions,
				List<Attribute> projectAttributes)
				throws OptimizationException {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean pushSelectDown(Expression predicate)
				throws SchemaMetadataException, AssertionError,
				TypeMappingException {
			// TODO Auto-generated method stub
			return false;
		}

		public void setPredicate(Expression newPredicate)
				throws SchemaMetadataException, AssertionError,
				TypeMappingException {
			// TODO Auto-generated method stub
			
		}

		public Operator shallowClone() {
			// TODO Auto-generated method stub
			return null;
		}

		public void addInput(Node n) {
			// TODO Auto-generated method stub
			
		}

		public void addOutput(Node n) {
			// TODO Auto-generated method stub
			
		}

		public String getID() {
			// TODO Auto-generated method stub
			return null;
		}

		public int getInDegree() {
			// TODO Auto-generated method stub
			return 0;
		}

		public List<Node> getInputsList() {
			// TODO Auto-generated method stub
			return null;
		}

		public int getOutDegree() {
			// TODO Auto-generated method stub
			return 0;
		}

		public Node[] getOutputs() {
			// TODO Auto-generated method stub
			return null;
		}

		public List<Node> getOutputsList() {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean hasOutput(Node n) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean isLeaf() {
			// TODO Auto-generated method stub
			return false;
		}

		public void removeInput(Node source) {
			// TODO Auto-generated method stub
			
		}

		public void removeOutput(Node target) {
			// TODO Auto-generated method stub
			
		}

		public void replaceInput(Node replace, Node newInput) {
			// TODO Auto-generated method stub
			
		}

		public void replaceOutput(Node replace, Node newOutput) {
			// TODO Auto-generated method stub
			
		}

		public void setInput(Node n, int index) {
			// TODO Auto-generated method stub
			
		}

		public void setOutput(Node n, int index) {
			// TODO Auto-generated method stub
			
		}

		public boolean isEquivalentTo(Node other) {
			// TODO Auto-generated method stub
			return false;
		}
}