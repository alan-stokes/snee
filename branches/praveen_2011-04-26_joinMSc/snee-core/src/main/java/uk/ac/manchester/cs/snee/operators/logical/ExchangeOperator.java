package uk.ac.manchester.cs.snee.operators.logical;

import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class ExchangeOperator extends LogicalOperatorImpl {

	private Logger logger = Logger.getLogger(this.getClass().getName());	
	

	public ExchangeOperator(LogicalOperator inputOperator, AttributeType boolType) {
		super(boolType);
		this.setOperatorName("EXCHANGE");
//		this.setNesCTemplateName("deliver");
		setChildren(new LogicalOperator[] {inputOperator});
		this.setOperatorDataType(inputOperator.getOperatorDataType());
		this.setOperatorSourceType(inputOperator.getOperatorSourceType());
		this.setParamStr("");
		
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Attribute> getAttributes() {
		return super.defaultGetAttributes();
	}

	@Override
	public List<Expression> getExpressions() {
		return super.defaultGetExpressions();
	}

	@Override
	public int getCardinality(CardinalityType card) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isAttributeSensitive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLocationSensitive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isRecursive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean acceptsPredicates() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean pushProjectionDown(List<Expression> projectExpressions,
			List<Attribute> projectAttributes) throws OptimizationException {
		// TODO Auto-generated method stub
		return getInput(0).pushProjectionDown(
				projectExpressions, projectAttributes);
	}

	@Override
	public boolean pushSelectDown(Expression predicate)
			throws SchemaMetadataException, AssertionError,
			TypeMappingException {
		// TODO Auto-generated method stub
		return getInput(0).pushSelectDown(predicate);
	}

	@Override
	public boolean isRemoveable() {		
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toString() {
		return this.getText() + " [ " + 
		super.getInput(0).toString() + " ]"; 
		
	}	

}
