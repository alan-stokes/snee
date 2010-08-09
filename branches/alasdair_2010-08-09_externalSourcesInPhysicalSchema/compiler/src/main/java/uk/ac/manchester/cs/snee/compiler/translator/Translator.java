package uk.ac.manchester.cs.snee.compiler.translator;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.Metadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParserTokenTypes;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlTreeWalker;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.AcquireOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.AggregationOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.AggregationType;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.DStreamOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.DeliverOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.IStreamOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.JoinOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.Operator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.OperatorDataType;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.ProjectOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.RStreamOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.ReceiveOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.SelectOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.UnionOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.operators.WindowOperator;
import antlr.RecognitionException;
import antlr.collections.AST;

public class Translator {

	Logger logger = Logger.getLogger(this.getClass().getName());

	private	Metadata _schemaMetadata;

	private Types _types;

	private AttributeType _boolType;

	public Translator (Metadata schemaMetadata) 
	throws TypeMappingException, SchemaMetadataException 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER Translator(), #extents=" + 
					schemaMetadata.getExtentNames().size());
		_schemaMetadata = schemaMetadata;
		_types = schemaMetadata.getTypes();
		_boolType = _boolType;
		if (logger.isDebugEnabled())
			logger.debug("RETURN Translator()");
	}

	private Operator translateFrom(AST ast) 
	throws ParserValidationException, SchemaMetadataException, 
	SourceDoesNotExistException, OptimizationException, ParserException, 
	TypeMappingException, ExtentDoesNotExistException, RecognitionException {
		if (logger.isTraceEnabled()) 
			logger.trace("ENTER translateFrom(): ast " + ast);
		AST source = ast.getFirstChild();
		Operator operator = translateExtents(source);
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN translateFrom(): operator " + operator);
		return operator;
	}

	private Operator translateExtents(AST ast) 
	throws ParserValidationException, SchemaMetadataException, 
	OptimizationException, SourceDoesNotExistException, ParserException, 
	TypeMappingException, ExtentDoesNotExistException, RecognitionException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER translateExtents() ast " + ast);
		if (ast == null) {
			String msg = "Extent list cannot be empty.";
			logger.error(msg);
			throw new ParserException(msg);
		}
		ArrayList<Operator> operators = new ArrayList<Operator>();
		AST nextAST = ast;
		while (nextAST != null) {
			Operator operator = translateExtent(nextAST);
			operators.add(operator);
			nextAST = nextAST.getNextSibling();
		}	
		Operator operator = combineSources (operators.toArray(
				new Operator[operators.size()]));
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN translateExtents() operator " + operator);
		return operator;
	}

	private Operator translateWindowAndLocalName(AST ast, 
			Operator operator) 
	throws ParserValidationException, OptimizationException, ParserException,
	RecognitionException { 
		if (logger.isTraceEnabled())
			logger.trace("ENTER translateWindowAndLocalName(): ast " +
					ast + " operator " + operator);
		if (ast == null)
			return operator;
		ASTPair pair;
		AST slideAST;
		AST slideUnit;
		int slide;
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.AT:
			AST atAST = ast;
			pair = findAST(ast, SNEEqlParserTokenTypes.SLIDE);
			slideAST = pair.getFirst();
			ast = pair.getNext();
			slideUnit = getUnit(slideAST, null);
			AST atUnit = getUnit(atAST, slideUnit);
			int at = translateWindowPart(atAST, atUnit);
			slide = translateWindowPart(slideAST, slideUnit);
			operator = createWindow(at, atUnit, at, atUnit, slide, 
					slideUnit, operator);
			break;
		case SNEEqlParserTokenTypes.FROM:
			AST fromAST = ast;
			pair = findAST(ast, SNEEqlParserTokenTypes.TO);
			AST toAST = pair.getFirst();
			pair = findAST(toAST, SNEEqlParserTokenTypes.SLIDE);
			slideAST = pair.getFirst();
			ast = pair.getNext();
			slideUnit = getUnit(slideAST, null);
			AST toUnit = getUnit(toAST, slideUnit);
			AST fromUnit = getUnit(fromAST, toUnit);
			int from = translateWindowPart(fromAST, fromUnit);
			int to = translateWindowPart(toAST, toUnit);
			slide = translateWindowPart(slideAST, slideUnit);
			operator = createWindow(from, fromUnit, to, toUnit, slide, 
					slideUnit, operator);
			break;
		case SNEEqlParserTokenTypes.FROM_OR_RANGE:
			AST scopeAST = ast;
			pair = findAST(ast, SNEEqlParserTokenTypes.SLIDE);
			slideAST = pair.getFirst();
			ast = pair.getNext();
			slideUnit = getUnit(slideAST, null);
			AST scopeUnit = getUnit(scopeAST, slideUnit);
			int scope = translateWindowPart(scopeAST, scopeUnit);
			slide = translateWindowPart(slideAST, slideUnit);
			if (scope > 0) //assume range
				operator = createWindow(1-scope, scopeUnit, 0, scopeUnit, 
						slide, slideUnit, operator);
			else //assume from
				operator = createWindow(scope, scopeUnit, 0, scopeUnit, 
						slide, slideUnit, operator);				
			break;
		case SNEEqlParserTokenTypes.Identifier:
			//No Window just a local name.
			break;
		case SNEEqlParserTokenTypes.NOW: 
			pair = findAST(ast, SNEEqlParserTokenTypes.SLIDE);
			slideAST = pair.getFirst();
			ast = pair.getNext();
			slideUnit = getUnit(slideAST, null);
			slide = translateWindowPart(slideAST, slideUnit);
			operator = createWindow(0, 0, true, slide, slideUnit, operator);
			break;
		case SNEEqlParserTokenTypes.RANGE:
			AST rangeAST = ast;
			pair = findAST(ast, SNEEqlParserTokenTypes.SLIDE);
			slideAST = pair.getFirst();
			ast = pair.getNext();
			slideUnit = getUnit(slideAST, null);
			AST rangeUnit = getUnit(rangeAST, slideUnit);
			int range = translateWindowPart(rangeAST, rangeUnit);
			slide = translateWindowPart(slideAST, slideUnit);
			operator = createWindow(1-range, rangeUnit, 0, rangeUnit, 
					slide, slideUnit, operator);
			break;
		default:
			throw new OptimizationException("Unprogrammed AST Type:" + 
					ast.getType() +" Text:"+ ast.getText());
		}
		translateLocalName(ast, operator);
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN translateWindowAndLocalName(): " +
					"operator " + operator);
		return operator;
	}

	private ASTPair findAST(AST ast, int type) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER findAST(): ast " + ast + " type " + type);
		ASTPair astPair;
		if (ast == null)
			logger.trace("AST is null");
		astPair = new ASTPair(null,null);
		AST toAST = ast.getNextSibling();
		if (toAST == null) {
			logger.trace("Second in pair is null");
			astPair = new ASTPair(null,null);
		} else if (toAST.getType() == type) {
			logger.trace("Create new pair with second as first.");
			astPair = new ASTPair(toAST,toAST.getNextSibling());
		} else {
			logger.trace("Create new pair as (null, second)");
			astPair = new ASTPair(null,toAST);
		}	
		if (logger.isTraceEnabled())
			logger.trace("RETURN findAST(): " + astPair);
		return astPair;
	}

	private WindowOperator createWindow (int from, AST fromUnit, int to, 
			AST toUnit, int slide, AST slideUnit, Operator child) 
	throws ParserValidationException, OptimizationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createWindow(): from " + from + " " + fromUnit +
					" to " + to + " " + toUnit + " slide " + slide + " " +
					slideUnit + " child " + child);
		boolean timeScope;
		if (isRowUnit(fromUnit))
			timeScope = false;
		else
			timeScope = true;
		if (toUnit != null){
			if (isRowUnit(toUnit)) {
				if (timeScope)
					throw new ParserValidationException("Can not make a " +
							"Window to unit of: " + toUnit.getText() +
							" with a from unit of " + fromUnit.getText());
			} else if (!timeScope)
				throw new ParserValidationException("Can not mike a Window " +
						"to unit of: " + toUnit.getText() + " with a from " +
						"unit of " + fromUnit.getText());
		}	
		WindowOperator window = createWindow (from, to, timeScope, slide, 
				slideUnit, child); 
		if (logger.isTraceEnabled())
			logger.trace("RETURN createWindow() with " + window);
		return window;
	}

	private WindowOperator createWindow (int from, int to, 
			boolean timeScope, int slide, AST slideUnit, Operator child) 
	throws ParserValidationException, OptimizationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER createWindow(): from " + from + " to " + to + 
					" isTimeScope " + timeScope +  " slide " + slide + " " +
					slideUnit + " child " + child);
		WindowOperator window;
		if (from > 0)
			throw new ParserValidationException("Window From value must be " +
					"less equal to zero. Found: " + from);
		if (to > 0)
			throw new ParserValidationException("Window To value must be " +
					"less equal to zero. Found: " + to);
		if (slide < 0)
			throw new ParserValidationException("Window Slide value must " +
					"be less equal or equal to zero. Found: " + slide);
		if (slide == 0)
			window = new WindowOperator(from, to, timeScope, 0, 0, child, _boolType);
		if (this.isRowUnit(slideUnit))
			window = new WindowOperator(from, to, timeScope, 0, slide, child, _boolType);
		window = new WindowOperator(from, to, timeScope, slide, 0, child, _boolType);
		if (logger.isTraceEnabled())
			logger.trace("RETURN createWindow()" + window);
		return window;
	}	

	private AST getUnit (AST ast, AST defaultUnit) 
	throws OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getUnit() ast: " + ast + "\tunit: " + defaultUnit);
		}
		if (ast == null || ast.getFirstChild() == null) {
			if (logger.isTraceEnabled())
				logger.trace("RETURN default unit " + defaultUnit);
			return defaultUnit;
		}
		if (logger.isTraceEnabled())
			logger.trace("First: " + ast.getFirstChild() + "\tnext: " + ast.getNextSibling());
		AST nextAST = ast.getFirstChild();
		while (nextAST.getNextSibling() != null) {
			nextAST = nextAST.getNextSibling();
			if (logger.isTraceEnabled())
				logger.trace("Next ast: " + nextAST);
		}
		switch (nextAST.getType()) {
		case SNEEqlParserTokenTypes.UNIT_NAME:
			if (logger.isTraceEnabled())
				logger.trace("RETURN getUnit() " + nextAST);
			return nextAST;
		case SNEEqlParserTokenTypes.DIV:
		case SNEEqlParserTokenTypes.Flt:
		case SNEEqlParserTokenTypes.Int:
		case SNEEqlParserTokenTypes.MINUS:
		case SNEEqlParserTokenTypes.MOD:
		case SNEEqlParserTokenTypes.MUL:
		case SNEEqlParserTokenTypes.NOW:
		case SNEEqlParserTokenTypes.PLUS:
		case SNEEqlParserTokenTypes.POW:
			//No Unit found
			if (logger.isTraceEnabled())
				logger.trace("RETURN getUnit() default unit " + defaultUnit);
			return defaultUnit;
		default:
		{
			String msg = "Unprogrammed AST Type:" + nextAST.getType() +
			" Text:"+ nextAST.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);
		}
		}
	}

	private int translateWindowPart(AST ast, AST unit) 
	throws ParserValidationException, ParserException,
	RecognitionException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER translateWindowPart()" + ast + " " + unit);
		if (ast == null) {
			if (logger.isTraceEnabled())
				logger.trace("RETURN 0");
			return 0;
		}
		//FIXME: FROM NOW TO NOW, NOW is removed by the parser
		if (unit == null) {
			String msg = "No Unit found with window declaration " +
			ast.getText();
			logger.warn(msg);
			throw new ParserValidationException(msg);		
		}
		AST expression = ast.getFirstChild();
		SNEEqlTreeWalker walker = new SNEEqlTreeWalker();
		double value = walker.expr(expression);
		assert (unit.getType() == SNEEqlParserTokenTypes.UNIT_NAME);
		value = convertToTick(value, unit.getText());
		if ((int)(value) != value) {
			String msg = "Window declration: "+ ast.toStringList() + 
			" does not convert to an integer tick";
			logger.warn(msg);
			throw new ParserValidationException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN translateWindowPart()" + value);
		return (int)value;
	}	

	public double convertToTick(double value, String unit) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER convertToTick()" + value + " " + unit);
		double result = value;
		if (isRowUnit(unit))
			result = value;
		int TICKS_PER_SECONDS = 1;
		if (unit.equalsIgnoreCase("seconds"))
			result = value * TICKS_PER_SECONDS;
		if (unit.equalsIgnoreCase("minutes"))
			result = value * TICKS_PER_SECONDS * 60;
		if (unit.equalsIgnoreCase("hours"))
			result = value * TICKS_PER_SECONDS * 3600;
		if (unit.equalsIgnoreCase("days"))
			result = value * TICKS_PER_SECONDS * 3600;
		if (logger.isDebugEnabled())
			logger.debug("RETURN convertToTick() " + result);
		return result;
	}

	private boolean isRowUnit(AST unit) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER isRowUnit() " + unit);
		boolean rowUnit = false;
		if (unit != null) {
			assert(unit.getType() == SNEEqlParserTokenTypes.UNIT_NAME);
			rowUnit = isRowUnit(unit.getText());
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN isRowUnit()" + rowUnit);
		return rowUnit;
	}

	private boolean isRowUnit(String name) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER isRowUnit() " + name);
		boolean rowUnit = false;
		if (name.equalsIgnoreCase("row"))
			rowUnit = true;
		else if (name.equalsIgnoreCase("rows"))
			rowUnit = true;
		else if (name.equalsIgnoreCase("tuple"))
			rowUnit = true;
		else if (name.equalsIgnoreCase("tuples"))
			rowUnit = true;
		if (logger.isTraceEnabled())
			logger.trace("RETURN isRowUnit()" + rowUnit);
		return rowUnit;
	}

	private void translateLocalName(AST ast, Operator operator) 
	throws OptimizationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER translateLocalName() with op=" + operator);
		if (ast == null)
			return;
		if (ast.getType() == SNEEqlParserTokenTypes.Identifier)
			operator.pushLocalNameDown(ast.getText());
		else {
			String msg = "Unprogrammed AST Type:" + ast.getType() +" Text:"+ ast.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN translateLocalName()");
	}

	private Operator combineSources (Operator[] operators)
	throws ParserValidationException, SchemaMetadataException, 
	OptimizationException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER combineSources(): number of operators " +
					operators.length);
		}
		if (logger.isDebugEnabled()) { 
			for (int i = 0; i < operators.length; i++)
				logger.debug("OP"+i+": "+operators[i]);
		}
		if (operators.length == 1) {
			if (logger.isDebugEnabled())
				logger.debug("RETURN combineSources(): Single Source " + operators[0]);
			return operators[0];
		}
		Operator temp;
		for (int i = 0; i < operators.length -1; i++)
			for (int j = i+1; j < operators.length; j++) {
				if ((operators[j].getOperatorDataType() == OperatorDataType.STREAM) || 
						((operators[i].getOperatorDataType() == OperatorDataType.RELATION) 
								&& (operators[j].getOperatorDataType() == OperatorDataType.WINDOWS))) {
					temp = operators[i];
					operators[i] = operators[j];
					operators[j] = temp;
				}
			}
		if (logger.isTraceEnabled()) {
			logger.trace("Operator list sorted");
			for (int i = 0; i < operators.length; i++)
				logger.trace("OP"+i+": "+operators[i]);
		}
		temp = operators[operators.length-1];
		for (int i = 0; i < operators.length-1; i++){
			if (operators[i].getOperatorDataType() == OperatorDataType.STREAM) {
				String msg = "Unable to join two streams";
				logger.warn(msg);
				throw new ParserValidationException(msg);
			}
			temp = new JoinOperator(operators[i], temp, _boolType);
			if (logger.isTraceEnabled())
				logger.trace("join "+i+": "+temp);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN combineSources() with " + temp);
		return temp;
	}

	private Operator translateExtent(AST ast) 
	throws ExtentDoesNotExistException, SchemaMetadataException, 
	TypeMappingException, SourceDoesNotExistException, 
	ParserValidationException, OptimizationException, ParserException,
	RecognitionException  
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER translateExtent() " + ast);
		ast.setText(ast.getText().toLowerCase());
		AST windowAST;
		Operator output;
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.RPAREN: 
			AST subQueryAST = ast.getFirstChild();
			output = translateQuery(subQueryAST);
			windowAST = subQueryAST.getNextSibling();
			break;
		case SNEEqlParserTokenTypes.SOURCE: 
			String extentName = ast.getText();
			ExtentMetadata sourceMetaData = 
				_schemaMetadata.getExtentMetadata(extentName);
			List<SourceMetadata> sources = 
				_schemaMetadata.getSources(extentName);
			switch (sourceMetaData.getExtentType()) {
			case SENSED:
				output = new AcquireOperator(extentName, extentName, 
					sourceMetaData, sources, _boolType);
				break;
			case PUSHED: 
				output = new ReceiveOperator(extentName, extentName, 
					sourceMetaData, sources, _boolType);
				break;
			default:
				String msg = "Unprogrammed ExtentType:" + sourceMetaData + 
					" Type:" + sourceMetaData.getExtentType();
				logger.warn(msg);
				throw new OptimizationException(msg);  	
			}
			windowAST = ast.getFirstChild();
			break;
		default:
			String msg = "Unprogrammed AST Type:" + ast.getType() + 
			" Text:" + ast.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);  	
		}
		output = translateWindowAndLocalName(windowAST, output);
		if (logger.isTraceEnabled())
			logger.trace("RETURN translateExtent() " + output);
		return output;
	}

	private Operator translateQuery(AST ast) 
	throws SchemaMetadataException, ParserValidationException, 
	OptimizationException, SourceDoesNotExistException, 
	ParserException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER translateQuery() " + ast + 
					" #children: " + ast.getNumberOfChildren());
		AST select = ast.getFirstChild();
		if (logger.isTraceEnabled())
			logger.trace("First child: " + select);
		if (select == null) {
			String msg = "No child in AST tree.";
			logger.error(msg);
			throw new ParserException(msg);
		}
		Operator operator;
		switch (select.getType()) {
		case SNEEqlParserTokenTypes.LPAREN: {
			logger.trace("Match LPAREN");
			logger.trace("Select Number of children: " + select.getNumberOfChildren());
			logger.trace("First child: " + select.getFirstChild());
			operator = translateQuery(select);
			break;
		}
		case SNEEqlParserTokenTypes.DSTREAM: {
			Operator inner = translateQuery(select);
			operator = new DStreamOperator(inner, _boolType);
			break;
		}
		case SNEEqlParserTokenTypes.ISTREAM: {
			Operator inner = translateQuery(select); 
			operator = new IStreamOperator(inner, _boolType);
			break;
		}
		case SNEEqlParserTokenTypes.RSTREAM: {
			Operator inner = translateQuery(select); 
			operator = new RStreamOperator(inner, _boolType);
			break;
		}
		case SNEEqlParserTokenTypes.SELECT:{
			AST from = select.getNextSibling();
			Operator fromOperator = translateFrom(from);
			//TODO apply predicate	  	  
			Operator preSelect = 
				applyWhereOrGroupBy(from.getNextSibling(), fromOperator);
			operator = translateSelect (select, preSelect);
			break;
		}
		case SNEEqlParserTokenTypes.UNION:{
			logger.trace("Union #children=" + select.getNumberOfChildren());
			AST firstChild = select.getFirstChild();
//			logger.trace("Left: " + firstChild);
//			logger.trace("Right: " + firstChild.getNextSibling());
			Operator left = translateQuery(firstChild);
			Operator right = translateQuery(firstChild.getNextSibling());
			operator = checkUnionCondition(left, right);
//			operator = new UnionOperator(left, right);
			break;
		}
		default:
		{
			String msg = "Unprogrammed AST Type: " + select.getType() + 
			" Text: " + select.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);  	
		}
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN translateQuery() " + operator);
		return operator;
	}

	private Operator checkUnionCondition(Operator left, Operator right) 
	throws ParserException, SchemaMetadataException, TypeMappingException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER checkUnionCondition()" +
					"\n\tleft: " + left +
					"\n\tright: " + right);
		if (left.getOperatorDataType() != OperatorDataType.STREAM ||
				right.getOperatorDataType() != OperatorDataType.STREAM) {
			String msg = "UNION only implemented for stream of tuples";
			logger.warn(msg);
			throw new ParserException(msg);
		}			
		logger.trace("Sub-queries both output streams of tuples.");
		List<Attribute> leftAttrs = left.getAttributes();
		List<Attribute> rightAttrs = right.getAttributes();
		if (leftAttrs.size() != rightAttrs.size()) {
			String msg = "Input streams are not union compatible. " +
					"Each sub query must have the same number of " +
					"select attributes.";
			throw new ParserException(msg);
		}
		logger.trace("Sub-queries have same number of output attributes");
		for (int i = 0; i < leftAttrs.size(); i++) {
			Attribute leftAttr = leftAttrs.get(i);
			AttributeType leftType = leftAttr.getType();
			Attribute rightAttr = rightAttrs.get(i);
			AttributeType rightType = rightAttr.getType();
			if (leftType != rightType) {
				String msg = "Input streams are not union compatible. " +
						"Attribute " + leftAttr.getAttributeName() + 
						" is not of the same type as " + 
						rightAttr.getAttributeName();
				logger.warn(msg);
				throw new ParserException(msg);
			}
		}
		logger.trace("Attributes are all of the same type.");
		Operator operator = new UnionOperator(left, right, _boolType);
		if (logger.isTraceEnabled())
			logger.trace("RETURN checkUnionCondition() with " + operator);
		return operator;
	}

	public LAF translate(AST ast, int queryID) 
	throws SchemaMetadataException, ParserValidationException, 
	OptimizationException, SourceDoesNotExistException,
	ParserException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER translate(): " + ast);
		DeliverOperator operator;
		if (ast==null) {
			String msg = "No parse tree available.";
			logger.error(msg);
			throw new ParserException(msg);
		} else {
			Operator queryRoot = translateQuery(ast);
			operator = new DeliverOperator(queryRoot, _boolType);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN translate() op=" + operator);
		LAF laf = new LAF(operator, "query" + queryID);
		return laf;
	}

	private Operator applyWhereOrGroupBy(AST ast, Operator input) 
	throws ParserValidationException, SchemaMetadataException, 
	TypeMappingException, OptimizationException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER applyWhereOrGroupBy() " + ast  + " " + input);
		if (ast == null) {
			if (logger.isTraceEnabled())
				logger.trace("RETURN applyWhereOrGroupBy() " + input);
			return input;
		}
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.WHERE:{
			Expression expression = 
				translateExpression(ast.getFirstChild(), input);
			if (logger.isTraceEnabled())
				logger.trace("Expression (" + expression + ") type: " + expression.getType());
			if (expression.getType() != _boolType) {
				String msg = "Illegal attempt to use a none boolean " +
				"expression in a where clause.";
				logger.warn(msg);
				throw new ParserValidationException(msg);
			}
			SelectOperator selectOperator = 
				new SelectOperator(expression,input, _boolType);
			if (logger.isTraceEnabled())
				logger.debug("RETURN applyWhereOrGroupBy() " + selectOperator);
			return selectOperator;
		}
		default:
		{
			String msg = "Unprogrammed AST Type:" + ast.getType() +
			" Text:"+ ast.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);  	
		}
		}	
	}

	private Operator translateSelect (AST ast, Operator input) 
	throws OptimizationException, ParserValidationException, 
	SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER translateSelect(): " + ast + " " + input);
		AST expressionAST = ast.getFirstChild();
		ArrayList<Expression> expressions = new ArrayList<Expression>();
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		boolean allowedInProjectOperator = true;
		boolean allowedInAggregationOperator = true;
		do {
			if (expressionAST.getType() == SNEEqlParserTokenTypes.STAR) {
				List<Attribute> incoming = input.getAttributes();
				expressions.addAll(incoming);
				attributes.addAll(incoming);
				allowedInAggregationOperator = false;
			} else {
				Expression expression = translateExpression (expressionAST, input);
				expressions.add(expression);
				Attribute attribute = expression.toAttribute();
				if (expressionAST.getType() == SNEEqlParserTokenTypes.AS) {
					AST attributeNameAST = expressionAST.getFirstChild().getNextSibling();
					assert(attributeNameAST.getType() == SNEEqlParserTokenTypes.ATTRIBUTE_NAME);
					String localName = attribute.getLocalName();
					String attributeName = attributeNameAST.getText();
					AttributeType type = attribute.getType();
					//new DataAttribute(attributeName, type);
					attribute = new DataAttribute(localName, attributeName, type);
				} 
				attributes.add(attribute);
				if (!expression.allowedInProjectOperator())
					allowedInProjectOperator = false;
				if (!expression.allowedInAggregationOperator())
					allowedInAggregationOperator = false;
			}	
			expressionAST = expressionAST.getNextSibling();
		} while(expressionAST != null);
		if (allowedInProjectOperator) {
			ProjectOperator projectOperator = 
				new ProjectOperator(expressions, attributes, input, _boolType);
			if (logger.isTraceEnabled())
				logger.trace("RETURN translateSelect() " + projectOperator);
			return projectOperator;
		}
		if (allowedInAggregationOperator)
			if (input.getOperatorDataType() != OperatorDataType.STREAM) {
				AggregationOperator aggregationOperator = 
					new AggregationOperator(expressions, attributes, input, _boolType);
				if (logger.isTraceEnabled())
					logger.trace("RETURN translateSelect() " + aggregationOperator);
				return aggregationOperator;
			}
		String msg = "Group By Having not yet programmed.";
		logger.warn(msg);
		throw new OptimizationException(msg);
	}

	private Expression translateExpression (AST ast, Operator input) 
	throws ParserValidationException, OptimizationException, NumberFormatException, TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER translateExpression() " + ast + " " + input);
		List<Attribute> attributes;
		Expression[] expressions;
		int count;
		AST child;
		Expression expression = null;
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.AS:
			if (logger.isTraceEnabled())
				logger.trace("Translate AS " + ast.getFirstChild());
			expression = 
				translateExpression (ast.getFirstChild(), input);
			break;
		case SNEEqlParserTokenTypes.Int:
			if (logger.isTraceEnabled())
				logger.trace("Translate Int " + ast.getText());
			expression = 
				new IntLiteral(Integer.parseInt(ast.getText()), 
						_types.getType("integer"));
			break;
		case SNEEqlParserTokenTypes.Flt:
			if (logger.isTraceEnabled())
				logger.trace("Translate Flt " + ast.getText());
			expression = 
				new FloatLiteral(Float.parseFloat(ast.getText()), 
						_types.getType("float"));
			break;
		case SNEEqlParserTokenTypes.Attribute:
			if (logger.isTraceEnabled())
				logger.trace("Translate Attribute " + ast.getText() +
						" attributes= " + input.getAttributes());
			String[] parts = ast.getText().split("[.]");
			assert (parts.length==2);
			logger.trace("Parts: " + parts[0] + " " + parts[1]);
			attributes = input.getAttributes();
			boolean attrFound = false;
			for (int i = 0; i< attributes.size(); i++) {
				Attribute attribute = attributes.get(i);
				logger.trace("Attribute: " + attribute);
				if (attribute.getLocalName().equalsIgnoreCase(parts[0]) && 
						attribute.getAttributeName().equalsIgnoreCase(parts[1])) {
					expression = attribute;
					attrFound = true;
					break;
				}
			}
			if (!attrFound) {
				String msg = "Unable to find Attribute " + ast.getText() + "||";
				logger.warn(msg);
				throw new ParserValidationException(msg);
			}
			break;
		case SNEEqlParserTokenTypes.Identifier:
			attributes = input.getAttributes();
			int found = -1;
			for (int i = 0; i< attributes.size(); i++) {
				if (attributes.get(i).getAttributeName().equalsIgnoreCase(ast.getText())) {
					if (found == -1)
						found = i;
					else {
						String msg = "Ambigious reference to unqualifeied attribute " +
						ast.getText();
						logger.warn(msg);
						throw new ParserValidationException(msg);
					}
				}
			}
			if (found != -1) {
				expression = attributes.get(found);
				break;
			} else {
				String msg = "Unable to find unqualified attribute "+ast.getText();
				logger.warn(msg);
				throw new ParserValidationException(msg);
			}
		case SNEEqlParserTokenTypes.FUNCTION_NAME:
			expression = this.getFunction(ast, input);
			break;		   
		case SNEEqlParserTokenTypes.DIV: 
		case SNEEqlParserTokenTypes.MINUS: 
		case SNEEqlParserTokenTypes.PLUS: 
		case SNEEqlParserTokenTypes.POW: 
		case SNEEqlParserTokenTypes.PRED: 
		case SNEEqlParserTokenTypes.MUL: 
		case SNEEqlParserTokenTypes.MOD: 
		case SNEEqlParserTokenTypes.OR: 
			expressions = new Expression[ast.getNumberOfChildren()];
			count = 0;
			child = ast.getFirstChild();
			while (child != null) {
				expressions[count] = translateExpression(child, input);
				count++;
				child = child.getNextSibling();
			}
			expression = 
				new MultiExpression (expressions, getMultiType(ast),
						_boolType);
			break;
		default:
		{
			String msg = "Unprogrammed AST Type: " + ast.getType() +
			" Text: "+ ast.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);  		
		}
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN translateExpression() " + expression);
		return expression;
	}

	private Expression getFunction(AST ast, Operator input) 
	throws ParserValidationException, OptimizationException, 
	TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTRY getFunction() " + ast + " " + input);
		assert(ast.getNumberOfChildren() == 1);
		assert(ast.getType() == SNEEqlParserTokenTypes.FUNCTION_NAME);
		Expression inner = translateExpression(ast.getFirstChild(), input);
		Expression expression;
		if ((ast.getText().equalsIgnoreCase("avg")) || (ast.getText().equalsIgnoreCase("average")))
			expression = new AggregationExpression(inner, AggregationType.AVG, _types.getType("integer"));
		else if (ast.getText().equalsIgnoreCase("count"))
			expression = new AggregationExpression(inner, AggregationType.COUNT, _types.getType("integer"));
		else if ((ast.getText().equalsIgnoreCase("minimum")) || (ast.getText().equalsIgnoreCase("min")))
			expression = new AggregationExpression(inner, AggregationType.MIN, _types.getType("integer"));
		else if ((ast.getText().equalsIgnoreCase("max")) || (ast.getText().equalsIgnoreCase("maximum")))
			expression = new AggregationExpression(inner, AggregationType.MAX, _types.getType("integer"));
		else if ((ast.getText().equalsIgnoreCase("sqr")) || (ast.getText().equalsIgnoreCase("square"))) {
			//FIXME: Not all arithmetic expressions are integers
			expression = new MultiExpression (new Expression[] {inner,inner}, MultiType.MULTIPLY, _types.getType("integer"));
		}
		else if ((ast.getText().equalsIgnoreCase("sqrt")) || (ast.getText().equalsIgnoreCase("squareroot"))) {
			//FIXME: Not all arithmetic expressions are integers			
			expression = new MultiExpression (new Expression[] {inner}, MultiType.SQUAREROOT, _types.getType("integer"));
		}
		else if (ast.getText().equalsIgnoreCase("sum"))
			expression = new AggregationExpression(inner, AggregationType.SUM, _types.getType("integer"));
		else 
			throw new OptimizationException("Unprogrammed Function name " +
					"AST Text:" + ast.getText());
		if (logger.isTraceEnabled())
			logger.trace("RETURN getFunction() " + expression);
		return expression;
	}

	private MultiType getMultiType(AST ast) 
	throws OptimizationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER getMultiType()");
		MultiType multiType;
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.DIV:
			multiType = MultiType.DIVIDE;
			break;
		case SNEEqlParserTokenTypes.MINUS: 
			multiType = MultiType.MINUS;
			break;
		case SNEEqlParserTokenTypes.PLUS: 
			multiType = MultiType.ADD;
			break;
		case SNEEqlParserTokenTypes.MUL: 
			multiType = MultiType.MULTIPLY;
			break;
		case SNEEqlParserTokenTypes.MOD: 
			multiType = MultiType.MOD;
			break;
		case SNEEqlParserTokenTypes.OR: 
			multiType = MultiType.OR;		   
			break;
		case SNEEqlParserTokenTypes.PRED:
			if (ast.getText().equals("="))
				multiType = MultiType.EQUALS;
			else if (ast.getText().equals("<"))
				multiType = MultiType.LESSTHAN;
			else if (ast.getText().equals(">"))
				multiType = MultiType.GREATERTHAN;
			else if (ast.getText().equals(">="))
				multiType = MultiType.GREATERTHANEQUALS;
			else if (ast.getText().equals("<="))
				multiType = MultiType.LESSTHANEQUALS;
			else if (ast.getText().equals("!="))
				multiType = MultiType.NOTEQUALS;
			else {
				String msg = "Unprogrammed PRED AST Text:" + ast.getText();
				logger.warn(msg);
				throw new OptimizationException(msg);
			}
			break;
		case SNEEqlParserTokenTypes.POW: 
			multiType = MultiType.POWER;
			break;
		default:
			String msg = "Unprogrammed AST Type: " + ast.getType() + 
			" Text: "+ ast.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);
		}	  
		if (logger.isTraceEnabled())
			logger.trace("RETURN getMultiType() " + multiType);
		return multiType;
	}

}


