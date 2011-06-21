package uk.ac.manchester.cs.snee.compiler.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.parser.ParserException;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlParserTokenTypes;
import uk.ac.manchester.cs.snee.compiler.parser.SNEEqlTreeWalker;
import uk.ac.manchester.cs.snee.compiler.queryplan.LAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.AggregationExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.EvalTimeAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.ExpressionException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.FloatLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IDAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IntLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiExpression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.MultiType;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.StringLiteral;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TimeAttribute;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationFunction;
import uk.ac.manchester.cs.snee.operators.logical.DStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.IStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.OperatorDataType;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.ReceiveOperator;
import uk.ac.manchester.cs.snee.operators.logical.ScanOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.UnionOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;
import uk.ac.manchester.cs.snee.types.Duration;
import antlr.RecognitionException;
import antlr.collections.AST;

public class Translator {

	Logger logger = Logger.getLogger(this.getClass().getName());

	private	MetadataManager _metadata;

	private Types _types;

	private AttributeType _boolType;

	/** Mappings from the different levels */
	private List< Map<String, String> > allLevelMappings;

	private Map<String, String> extentNameMappings;

	public Translator (MetadataManager metadata) 
	throws TypeMappingException, SchemaMetadataException 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER Translator()");
		}
		_metadata = metadata;
		if (logger.isTraceEnabled()) {
			logger.trace("#extents=" + metadata.getExtentNames().size());
		}
		_types = metadata.getTypes();
		_boolType = _types.getType("boolean");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN Translator()");
		}
	}

	private LogicalOperator translateFrom(AST ast) 
	throws ExpressionException, SchemaMetadataException, 
	SourceDoesNotExistException, OptimizationException, ParserException, 
	TypeMappingException, ExtentDoesNotExistException, 
	RecognitionException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateFrom(): ast " +
					ast.toStringList());
		}
		AST source = ast.getFirstChild();
		LogicalOperator operator = translateExtents(source);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateFrom(): operator " + 
					operator);
		}
		return operator;
	}

	private LogicalOperator translateExtents(AST ast) 
	throws ExpressionException, SchemaMetadataException, 
	OptimizationException, SourceDoesNotExistException, ParserException, 
	TypeMappingException, ExtentDoesNotExistException, 
	RecognitionException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateExtents() ast " + 
					ast);
		}
		if (ast == null) {
			String msg = "Extent list cannot be empty.";
			logger.error(msg);
			throw new ParserException(msg);
		}
		List<LogicalOperator> operators = 
			new ArrayList<LogicalOperator>();
		AST nextAST = ast;
		while (nextAST != null) {
			LogicalOperator operator = translateExtent(nextAST);
			operators.add(operator);
			nextAST = nextAST.getNextSibling();
		}	
		LogicalOperator operator = 
			combineSources (operators.toArray(
				new LogicalOperator[operators.size()]));
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateExtents() operator " + 
					operator);
		}
		return operator;
	}

	private LogicalOperator translateWindowAndLocalName(AST ast, 
			LogicalOperator operator, String extentName) 
	throws ExpressionException, OptimizationException, 
	ParserException, RecognitionException { 
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateWindowAndLocalName(): ast " +
					ast + " operator " + operator);
		}
		if (ast == null) {
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN translateWindowAndLocalName() " +
						"with " + operator);
			}
			return operator;
		}
		ASTPair pair;
		AST slideAST;
		AST slideUnit;
		int slide;
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.AT:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate AT window");
			}
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
			if (logger.isTraceEnabled()) {
				logger.trace("Translate FROM window");
			}
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
			if (logger.isTraceEnabled()) {
				logger.trace("Translate FROM or RANGE window");
			}
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
			if (logger.isTraceEnabled()) {
				logger.trace("Identifier, do nothing");
			}
			//No Window just a local name.
			//FIXME: In this case, rename the attributes label, to
			//that of the local name that is presented here
			break;
		case SNEEqlParserTokenTypes.NOW: 
			if (logger.isTraceEnabled()) {
				logger.trace("Translate NOW window");
			}
			pair = findAST(ast, SNEEqlParserTokenTypes.SLIDE);
			slideAST = pair.getFirst();
			ast = pair.getNext();
			slideUnit = getUnit(slideAST, null);
			slide = translateWindowPart(slideAST, slideUnit);
			operator = createWindow(0, 0, true, slide, slideUnit, operator);
			break;
		case SNEEqlParserTokenTypes.RANGE:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate RANGE window");
			}
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
		case SNEEqlParserTokenTypes.RESCAN:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate RESCAN window");
			}
			Duration rescanInterval = translateRescanWindow(ast);
			((ScanOperator)operator).setRescanInterval(rescanInterval);
			ast = ast.getNextSibling();
			break;
		default:
			String message = "Unprogrammed AST Type:" + 
					ast.getType() +" Text:"+ ast.getText();
			logger.warn(message);
			throw new OptimizationException(message);
		}
		translateLocalName(ast, operator, extentName);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateWindowAndLocalName(): " +
					"operator " + operator);
		}
		return operator;
	}
	
	private Duration translateRescanWindow(AST ast) 
	throws RecognitionException, ParserException, ExpressionException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateRescanWindow() with " + ast + 
					" (" + ast.getFirstChild() + ", " + 
					ast.getFirstChild().getNextSibling() + ")");
		}
		AST expression = ast.getFirstChild();
		SNEEqlTreeWalker walker = new SNEEqlTreeWalker();
		double value = walker.expr(expression);
		AST unit = expression.getNextSibling();
		Duration duration = null;
		if (unit.getType() != SNEEqlParserTokenTypes.UNIT_NAME) {
			String msg = "No Unit found with window declaration " +
				ast.getText();
			logger.warn(msg);
			throw new ExpressionException(msg);
		} else if (unit.getText().equalsIgnoreCase("millisecond") || 
				unit.getText().equalsIgnoreCase("milliseconds")) {
			duration  = new Duration((long) value, TimeUnit.MILLISECONDS);
		} else if (unit.getText().equalsIgnoreCase("second") || 
				unit.getText().equalsIgnoreCase("seconds")) {
			duration  = new Duration((long) value, TimeUnit.SECONDS);
		} else if (unit.getText().equalsIgnoreCase("minute") || 
				unit.getText().equalsIgnoreCase("minutes")) {
			duration  = new Duration((long) value, TimeUnit.MINUTES);
		} else if (unit.getText().equalsIgnoreCase("hour") || 
				unit.getText().equalsIgnoreCase("hours")) {
			duration  = new Duration((long) value, TimeUnit.HOURS);
		} else if (unit.getText().equalsIgnoreCase("day") || 
				unit.getText().equalsIgnoreCase("days")) {
			duration  = new Duration((long) value, TimeUnit.DAYS);
		} else {
			String msg = "Unsupported time unit " + unit.getText();
			logger.warn(msg);
			throw new ExpressionException(msg);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateRescanWindow() with " + duration);
		}
		return duration;
	}

	private ASTPair findAST(AST ast, int type) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER findAST(): ast " + ast + 
					" type " + type);
		}
		ASTPair astPair;
		if (ast == null) {
			logger.trace("AST is null");
		}
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
			AST toUnit, int slide, AST slideUnit, LogicalOperator child) 
	throws ExpressionException, OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createWindow(): " +
					"from " + from + " " + fromUnit +
					" to " + to + " " + toUnit + 
					" slide " + slide + " " +
					slideUnit + " child " + child);
		}
		boolean timeScope;
		if (isRowUnit(fromUnit)) {
			timeScope = false;
		} else {
			timeScope = true;
		}
		if (toUnit != null) {
			if (isRowUnit(toUnit)) {
				if (timeScope) {
					String message = "Can not make a " +
							"Window to unit of: " + toUnit.getText() +
							" with a from unit of " + fromUnit.getText();
					logger.warn(message);
					throw new ExpressionException(message);
				}
			} else if (!timeScope) {
				String message = "Can not make a Window " +
						"to unit of: " + toUnit.getText() + 
						" with a from " +
						"unit of " + fromUnit.getText();
				logger.warn(message);
				throw new ExpressionException(message);
			}
		}	
		WindowOperator window = 
			createWindow (from, to, timeScope, slide, slideUnit, child); 
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createWindow() with " + window);
		}
		return window;
	}

	private WindowOperator createWindow (int from, int to, 
			boolean timeScope, int slide, AST slideUnit,
			LogicalOperator child) 
	throws ExpressionException, OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER createWindow(): " + "from " + from + 
					" to " + to + " isTimeScope " + timeScope +  
					" slide " + slide + " " +
					slideUnit + " child " + child);
		}
		WindowOperator window;
		if (from > 0) {
			String message = "Window From value must be " +
					"less equal to zero. Found: " + from;
			logger.warn(message);
			throw new ExpressionException(message);
		}
		if (to > 0) {
			String message = "Window To value must be " +
					"less equal to zero. Found: " + to;
			logger.warn(message);
			throw new ExpressionException(message);
		}
		if (slide < 0) {
			String message = "Window Slide value must " +
					"be less equal or equal to zero. Found: " + slide;
			logger.warn(message);
			throw new ExpressionException(message);
		}
		if (slide == 0) {
			window = new WindowOperator(from, to, timeScope, 0, 0, 
					child, _boolType);
		}
		if (this.isRowUnit(slideUnit)) {
			window = new WindowOperator(from, to, timeScope, 0, slide, 
					child, _boolType);
		}
		//XXX-AG: Surely this overrides the earlier windows?
		window = new WindowOperator(from, to, timeScope, slide, 0, 
				child, _boolType);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN createWindow()" + window);
		}
		return window;
	}	

	private AST getUnit (AST ast, AST defaultUnit) 
	throws OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getUnit() ast: " + ast + "\tunit: " + 
					defaultUnit);
		}
		if (ast == null || ast.getFirstChild() == null) {
			if (logger.isTraceEnabled())
				logger.trace("RETURN default unit " + defaultUnit);
			return defaultUnit;
		}
		if (logger.isTraceEnabled()) {
			logger.trace("First: " + ast.getFirstChild() + 
					"\tnext: " + ast.getNextSibling());
		}
		AST nextAST = ast.getFirstChild();
		while (nextAST.getNextSibling() != null) {
			nextAST = nextAST.getNextSibling();
			if (logger.isTraceEnabled()) {
				logger.trace("Next ast: " + nextAST);
			}
		}
		switch (nextAST.getType()) {
		case SNEEqlParserTokenTypes.UNIT_NAME:
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN getUnit() " + nextAST);
			}
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
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN getUnit() default unit " + 
						defaultUnit);
			}
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
	throws ExpressionException, ParserException,
	RecognitionException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateWindowPart()" + 
					ast + " " + unit);
		}
		if (ast == null) {
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN 0");
			}
			return 0;
		}
		//FIXME: FROM NOW TO NOW, NOW is removed by the parser
		if (unit == null) {
			String msg = "No Unit found with window declaration " +
				ast.getText();
			logger.warn(msg);
			throw new ExpressionException(msg);		
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
			throw new ExpressionException(msg);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateWindowPart()" + value);
		}
		return (int)value;
	}	

	public double convertToTick(double value, String unit) {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER convertToTick()" + value + " " + unit);
		}
		double result = value;
		if (isRowUnit(unit)) {
			result = value;
		}
		int TICKS_PER_SECOND = 1000;
		if (unit.equalsIgnoreCase("seconds")) {
			result = value * TICKS_PER_SECOND;
		} else if (unit.equalsIgnoreCase("minutes")) {
			result = value * TICKS_PER_SECOND * 60;
		} else if (unit.equalsIgnoreCase("hours")) {
			result = value * TICKS_PER_SECOND * 3600;
		} else if (unit.equalsIgnoreCase("days")) {
			result = value * TICKS_PER_SECOND * 3600 * 24;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN convertToTick() " + result);
		}
		return result;
	}

	private boolean isRowUnit(AST unit) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER isRowUnit() " + unit);
		}
		boolean rowUnit = false;
		if (unit != null) {
			assert(unit.getType() == SNEEqlParserTokenTypes.UNIT_NAME);
			rowUnit = isRowUnit(unit.getText());
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN isRowUnit()" + rowUnit);
		}
		return rowUnit;
	}

	private boolean isRowUnit(String name) {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER isRowUnit() " + name);
		}
		boolean rowUnit = false;
		if (name.equalsIgnoreCase("row")) {
			rowUnit = true;
		} else if (name.equalsIgnoreCase("rows")) {
			rowUnit = true;
		} else if (name.equalsIgnoreCase("tuple")) {
			rowUnit = true;
		} else if (name.equalsIgnoreCase("tuples")) {
			rowUnit = true;
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN isRowUnit()" + rowUnit);
		}
		return rowUnit;
	}

	private void translateLocalName(AST ast, LogicalOperator operator,
			String extentName) 
	throws OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateLocalName() with alias=" + ast + " op=" + 
					operator);
		}
		if (ast == null) {
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN translateLocalName()");
			}
			return;
		}
		String extentReference = ast.getText();
		if (ast.getType() == SNEEqlParserTokenTypes.Identifier) {
			if (logger.isTraceEnabled()) {
				logger.trace("Extent " + extentName + 
						" referenced as " + extentReference);
			}
			extentNameMappings.put(extentReference, extentName);

			/* BUG FIX: If the extentName is empty but the extentReference
			 * is not, then (most likely) we are in a sub-query. In this case
			 * we need to rename the attributes with the extent reference.
			 * We do this by applying the name to the attribute label */
			List<Attribute> attributes = 
				(ArrayList<Attribute>) operator.getAttributes();

			/* For every attribute, apply a rename on the display name */
			for ( Attribute attribute : attributes ){

				String newAttrName = null;
				String attrName = attribute.getAttributeDisplayName();
				String[] parts = attrName.split("[.]");

				assert ( parts.length > 0 && parts.length <= 2 );

				/* There is not extent name, so this is a sub-query */
				if ( extentName.isEmpty() ){

					if ( parts.length == 1 ){
						/* There is only the name of the attribute */
						newAttrName = extentReference + "." + parts[0];
	
					} else {
	
						/* There are both a name and another extent
						 * reference */
						newAttrName = extentReference + "." + parts[1];
					}
					
				}else{

					/* This is not a sub-query and the first part of the
					 * display name is that of the extent name */
					if ( parts[0].equalsIgnoreCase(extentName) ){

						/* Rename according to the extentReference */
						newAttrName = attrName.replace(parts[0], extentReference);

					}
				}

				attribute.setAttributeDisplayName(newAttrName);
			}
			//XXX: Removed by AG as metadata now handled in metadata object
//			operator.pushLocalNameDown(extentReference);
		} else {
			String msg = "Unprogrammed AST Type:" + ast.getType() +
				" Text:"+ extentReference;
			logger.warn(msg);
			throw new OptimizationException(msg);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateLocalName()");
		}
	}

	private LogicalOperator combineSources (LogicalOperator[] operators)
	throws ExpressionException, SchemaMetadataException, 
	OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER combineSources(): " +
					"number of operators " + operators.length);
		}
		if (logger.isDebugEnabled()) { 
			for (int i = 0; i < operators.length; i++) {
				logger.debug("OP"+i+": "+operators[i]);
			}
		}
		if (operators.length == 1) {
			if (logger.isDebugEnabled()) {
				logger.debug("RETURN combineSources(): " +
						"Single Source " + operators[0]);
			}
			return operators[0];
		}
		LogicalOperator temp;
		for (int i = 0; i < operators.length -1; i++) {
			for (int j = i+1; j < operators.length; j++) {
				if ((operators[j].getOperatorDataType() == OperatorDataType.STREAM) || 
						((operators[i].getOperatorDataType() == OperatorDataType.RELATION) 
								&& (operators[j].getOperatorDataType() == OperatorDataType.WINDOWS))) {
					temp = operators[i];
					operators[i] = operators[j];
					operators[j] = temp;
				}
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Operator list sorted");
			for (int i = 0; i < operators.length; i++) {
				logger.trace("OP"+i+": "+operators[i]);
			}
		}
		temp = operators[operators.length-1];
		for (int i = 0; i < operators.length-1; i++) {
			if (operators[i].getOperatorDataType() == OperatorDataType.STREAM &&
					temp.getOperatorDataType() == OperatorDataType.STREAM) {
				String msg = "Unable to join two streams";
				logger.warn(msg);
				throw new ExpressionException(msg);
			} else if (operators[i].getOperatorDataType() == OperatorDataType.STREAM &&
					temp.getOperatorDataType() == OperatorDataType.WINDOWS) {
				String msg = "Unable to join a stream and a window, " +
						"both stream extents need to have a window declared.";
				logger.warn(msg);
				throw new ExpressionException(msg);
			}
			temp = new JoinOperator(operators[i], temp, _boolType);
			if (logger.isTraceEnabled()) {
				logger.trace("join "+i+": "+temp);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN combineSources() with " + temp);
		}
		return temp;
	}

	private LogicalOperator translateExtent(AST ast) 
	throws ExtentDoesNotExistException, SchemaMetadataException, 
	TypeMappingException, SourceDoesNotExistException, 
	ExpressionException, OptimizationException, ParserException,
	RecognitionException  
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateExtent() " + 
					ast.toStringList());
		}
		ast.setText(ast.getText().toLowerCase());
		AST windowAST;
		LogicalOperator output;
		String extentName;
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.RPAREN: 
			if (logger.isTraceEnabled()) {
				logger.trace("Translate RPAREN");
			}

			/* BUG FIX: 9/2/2011
			 * Given that we encountered a subquery, we should begin
			 * with a new map of the extents */
			extentNameMappings = new HashMap<String, String>();
			allLevelMappings.add(extentNameMappings);

			AST subQueryAST = ast.getFirstChild();
			output = translateQuery(subQueryAST);
			windowAST = subQueryAST.getNextSibling();
			extentName = "";

			/* Remove extent mappings that were due to
			 * nested queries */
			extentNameMappings.clear();
			allLevelMappings.remove(allLevelMappings.size() - 1);
			extentNameMappings = allLevelMappings.get(allLevelMappings.size() - 1);

			break;
		case SNEEqlParserTokenTypes.SOURCE: 
			if (logger.isTraceEnabled()) {
				logger.trace("Translate SOURCE: " + ast.getText());
			}
			extentName = ast.getText();
			ExtentMetadata extentMetadata = 
				_metadata.getExtentMetadata(extentName);
			SourceMetadataAbstract source = 
				_metadata.getSource(extentName);
			switch (extentMetadata.getExtentType()) {
			case SENSED:
				if (logger.isTraceEnabled()) {
					logger.trace("Translate SENSED stream");
				}
				output = new AcquireOperator(extentMetadata, 
						_metadata.getTypes(), source, _boolType);
				break;
			case PUSHED: 
				if (logger.isTraceEnabled()) {
					logger.trace("Translate PUSHED stream");
				}
				output = new ReceiveOperator(extentMetadata, source, 
						_boolType);
				break;
			case TABLE:
				if (logger.isTraceEnabled()) {
					logger.trace("Translate TABLE");
				}
				output = new ScanOperator(extentMetadata, source,
						_boolType);
				break;
			default:
				String msg = "Unprogrammed ExtentType:" + 
					extentMetadata + " Type:" + 
					extentMetadata.getExtentType();
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
		output = translateWindowAndLocalName(windowAST, output, 
				extentName);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateExtent() " + output);
		}
		return output;
	}

	private LogicalOperator translateQuery(AST ast) 
	throws SchemaMetadataException, ExpressionException, 
	OptimizationException, SourceDoesNotExistException, 
	ParserException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateQuery() with '" +
					ast +
					"' #children: " + ast.getNumberOfChildren() +
					"\n" + ast.toStringList());
		}
		AST node = ast.getFirstChild();
		if (logger.isTraceEnabled()) {
			logger.trace("First child: " + node);
		}
		if (node == null) {
			String msg = "No child in AST tree.";
			logger.error(msg);
			throw new ParserException(msg);
		}
		LogicalOperator operator;
		switch (node.getType()) {
		case SNEEqlParserTokenTypes.LPAREN: {
			if (logger.isTraceEnabled()) {				
				logger.trace("Translate LPAREN");
				logger.trace("Select Number of children: " + 
						node.getNumberOfChildren());
			}
			operator = translateQuery(node);
			break;
		}
		case SNEEqlParserTokenTypes.DSTREAM: {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate DSTREAM query");
			}
			LogicalOperator inner = translateQuery(node);
			operator = new DStreamOperator(inner, _boolType);
			break;
		}
		case SNEEqlParserTokenTypes.ISTREAM: {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate ISTREAM query");
			}
			LogicalOperator inner = translateQuery(node); 
			operator = new IStreamOperator(inner, _boolType);
			break;
		}
		case SNEEqlParserTokenTypes.RSTREAM: {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate RSTREAM query");
			}
			LogicalOperator inner = translateQuery(node); 
			operator = new RStreamOperator(inner, _boolType);
			break;
		}
		case SNEEqlParserTokenTypes.SELECT:{
			if (logger.isTraceEnabled()) {
				logger.trace("Translate SELECT query");
			}
			AST from = node.getNextSibling();
			LogicalOperator fromOperator = translateFrom(from);
			//TODO apply predicate	  	  
			LogicalOperator preSelect = 
				applyWhereOrGroupBy(from.getNextSibling(), fromOperator);
			operator = translateSelect (node, preSelect);
			break;
		}
		case SNEEqlParserTokenTypes.UNION:{
			if (logger.isTraceEnabled()) {
				logger.trace("Translate UNION query");
			}
			AST leftChild = node.getFirstChild();
			AST rightChild = leftChild.getNextSibling();
			LogicalOperator left;
			/* 
			 * Need to handle nested UNIONs differently from
			 * the nested sub-query. 
			 */
			if (leftChild.getType() == SNEEqlParserTokenTypes.UNION) {
				left = translateQuery(node);
			} else {
				left = translateQuery(leftChild);
			}
			LogicalOperator right = translateQuery(rightChild);
			operator = checkUnionCondition(left, right);
			break;
		}
		default:
		{
			String msg = "Unprogrammed AST Type: " + node.getType() + 
				" Text: " + node.getText();
			logger.warn(msg);
			throw new OptimizationException(msg);  	
		}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN translateQuery() " + operator);
		}
		return operator;
	}

	private LogicalOperator checkUnionCondition(LogicalOperator left, 
			LogicalOperator right) 
	throws ParserException, SchemaMetadataException, 
	TypeMappingException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER checkUnionCondition()" +
					"\n\tleft: " + left +
					"\n\tright: " + right);
		}
		if (left.getOperatorDataType() != OperatorDataType.STREAM ||
				right.getOperatorDataType() != OperatorDataType.STREAM) {
			String msg = "UNION only implemented for stream of tuples";
			logger.warn(msg);
			throw new ParserException(msg);
		}			
		if (logger.isTraceEnabled()) {
			logger.trace("Sub-queries both output streams of tuples.");
		}
		List<Attribute> leftAttrs = left.getAttributes();
		List<Attribute> rightAttrs = right.getAttributes();
		if (leftAttrs.size() != rightAttrs.size()) {
			String msg = "Input streams are not union compatible. " +
					"Each sub query must have the same number of " +
					"select attributes.";
			logger.warn(msg);
			throw new ParserException(msg);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Sub-queries have same number of output " +
					"attributes");
		}
		for (int i = 0; i < leftAttrs.size(); i++) {
			Attribute leftAttr = leftAttrs.get(i);
			AttributeType leftType = leftAttr.getType();
			Attribute rightAttr = rightAttrs.get(i);
			AttributeType rightType = rightAttr.getType();
			if (leftType != rightType) {
				String msg = "Input streams are not union compatible. " +
						"Attribute " + leftAttr.getAttributeDisplayName() + 
						" is not of the same type as " + 
						rightAttr.getAttributeDisplayName();
				logger.warn(msg);
				throw new ParserException(msg);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Attributes are all of the same type.");
		}
		LogicalOperator operator = 
			new UnionOperator(left, right, _boolType);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN checkUnionCondition() with " +
					operator);
		}
		return operator;
	}

	public LAF translate(AST ast, int queryID) 
	throws SchemaMetadataException, ExpressionException, 
	OptimizationException, SourceDoesNotExistException,
	ParserException, TypeMappingException, ExtentDoesNotExistException,
	RecognitionException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER translate(): " + 
					ast.toStringTree() +
					" #children=" + ast.getNumberOfChildren());
		}

		// Create new empty map for extent name mappings
		allLevelMappings = new ArrayList<Map<String,String>>(5);
		extentNameMappings = new HashMap<String, String>();

		allLevelMappings.add(extentNameMappings);

		DeliverOperator operator;
		if (ast==null) {

			allLevelMappings = null;
			extentNameMappings = null;

			String msg = "No parse tree available.";
			logger.warn(msg);
			throw new ParserException(msg);
		} else {
			LogicalOperator queryRoot = translateQuery(ast);
			operator = new DeliverOperator(queryRoot, _boolType);
		}
		LAF laf = new LAF(operator, "query" + queryID);
		if (logger.isTraceEnabled()) {
			StringBuffer buffer = 
				new StringBuffer("LAF " + laf.getID());
			Iterator<LogicalOperator> it = 
				laf.operatorIterator(TraversalOrder.PRE_ORDER);
			while (it.hasNext()) {
				buffer.append(it.next().toString());
				buffer.append("\n");
			}
			logger.trace(buffer.toString());
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN translate() laf=" + 
					laf.getID());
		}
		return laf;
	}

	private LogicalOperator applyWhereOrGroupBy(AST ast, 
			LogicalOperator input) 
	throws ExpressionException, SchemaMetadataException, 
	TypeMappingException, OptimizationException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER applyWhereOrGroupBy() " + 
					ast  + " " + input);
		}
		if (ast == null) {
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN applyWhereOrGroupBy() " + input);
			}
			return input;
		}
		switch (ast.getType()) {
		case SNEEqlParserTokenTypes.WHERE:{
			if (logger.isTraceEnabled()) {
				logger.trace("Translate WHERE");
			}
			Expression expression = 
				translateExpression(ast.getFirstChild(), input);
			if (logger.isTraceEnabled()) {
				logger.trace("Expression (" + expression + 
						") type: " + expression.getType());
			}
			if (expression.getType() != _boolType) {
				String msg = "Illegal attempt to use a none boolean " +
					"expression in a where clause.";
				logger.warn(msg);
				throw new ExpressionException(msg);
			}
			SelectOperator selectOperator = 
				new SelectOperator(expression,input, _boolType);
			if (logger.isTraceEnabled()) {
				logger.debug("RETURN applyWhereOrGroupBy() " + 
						selectOperator);
			}
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

	private LogicalOperator translateSelect (AST ast, 
			LogicalOperator input) 
	throws OptimizationException, ExpressionException, 
	SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateSelect(): " +
					"" + ast.toStringList() + " " + input);
		}
		AST expressionAST = ast.getFirstChild();
		if (expressionAST == null) {
			String msg = "Invalid SELECT clause syntax";
			logger.warn(msg);
			throw new ExpressionException(msg);
		}
		List<Expression> expressions = new ArrayList<Expression>();
		List<Attribute> attributes = new ArrayList<Attribute>();
		boolean allowedInProjectOperator = true;
		boolean allowedInAggregationOperator = true;
		loop: //label to enable breaking out from inner case statement
		do {
			int expType = expressionAST.getType();
			Expression expression;
			Attribute attribute;
			switch (expType) {
			case SNEEqlParserTokenTypes.STAR:
				if (logger.isTraceEnabled()) {
					logger.trace("project to all attribtues");
				}
				/* STAR must be the only token in the select clause */
				if (expressions.isEmpty() && 
						expressionAST.getNextSibling() == null) {
					List<Attribute> incoming = input.getAttributes();
					expressions.addAll(incoming);
					attributes.addAll(incoming);
					allowedInAggregationOperator = false;
				} else {
					String msg = "Invalid SELECT Clause syntax. " +
							"\'*\' must be only attribute";
					logger.warn(msg);
					throw new ExpressionException(msg);
				}
				// Break out of the while loop as we're done!
				break loop;
			case SNEEqlParserTokenTypes.AS:
				if (logger.isTraceEnabled()) {
					logger.trace("rename in select clause, Translate AS " + 
							expressionAST.getFirstChild());
				}
				expression = 
					translateExpression(expressionAST.getFirstChild(), input);
					attribute = expression.toAttribute();
					attribute = translateAttributeRename(
							expressionAST.getFirstChild().getNextSibling(), 
							attribute);
				break;
			default:
				if (logger.isTraceEnabled()) {
					logger.trace("project to specified attributes");
				}
				expression = 
					translateExpression(expressionAST, input);
				attribute = expression.toAttribute();
				if (!expression.allowedInProjectOperator()) {
					allowedInProjectOperator = false;
				}
				if (!expression.allowedInAggregationOperator()) {
					allowedInAggregationOperator = false;
				}
				break;
			}
			expressions.add(expression);
			attributes.add(attribute);
			expressionAST = expressionAST.getNextSibling();
		} while(expressionAST != null);
		if (allowedInProjectOperator) {
			ProjectOperator projectOperator = 
				new ProjectOperator(expressions, attributes, 
						input, _boolType);
			if (logger.isTraceEnabled()) {
				logger.trace("RETURN translateSelect() " + 
						projectOperator);
			}
			return projectOperator;
		}
		if (allowedInAggregationOperator) {
			if (input.getOperatorDataType() != OperatorDataType.STREAM) {
				AggregationOperator aggregationOperator = 
					new AggregationOperator(expressions, attributes, 
							input, _boolType);
				if (logger.isTraceEnabled()) {
					logger.trace("RETURN translateSelect() " + 
							aggregationOperator);
				}
				return aggregationOperator;
			}
		}
		String msg = "Group By Having not yet programmed.";
		logger.warn(msg);
		throw new OptimizationException(msg);
	}

	private Attribute translateAttributeRename(AST attributeNameAST,
			Attribute attribute) 
	throws SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateAttributeRename() with " +
					attributeNameAST + " " + attribute);
		}
		assert(attributeNameAST.getType() == 
			SNEEqlParserTokenTypes.ATTRIBUTE_NAME);
		attribute.setAttributeDisplayName(attributeNameAST.getText());
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateAttributeRename() with " +
					attribute);
		}
		return attribute;
	}

	private Expression translateExpression (AST ast, 
			LogicalOperator input) 
	throws ExpressionException, OptimizationException, 
	NumberFormatException, TypeMappingException, 
	SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER translateExpression() " + 
					ast.toStringList() + " " + input);
		}
		List<Attribute> attributes;
		Expression[] expressions;
		int count;
		AST child;
		Expression expression = null;
		switch (ast.getType()) {
//		case SNEEqlParserTokenTypes.AS:
//			if (logger.isTraceEnabled()) {
//				logger.trace("Translate AS " + ast.getFirstChild());
//			}
//			expression = 
//				translateExpression(ast.getFirstChild(), input);
//			break;
		case SNEEqlParserTokenTypes.Int:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate Int " + ast.getText());
			}
			expression = 
				new IntLiteral(Integer.parseInt(ast.getText()), 
						_types.getType("integer"));
			expression.setIsConstant(true);
			break;
		case SNEEqlParserTokenTypes.Flt:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate Flt " + ast.getText());
			}
			expression = 
				new FloatLiteral(Float.parseFloat(ast.getText()), 
						_types.getType("float"));
			expression.setIsConstant(true);
			break;
		case SNEEqlParserTokenTypes.QuotedString:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate QuotedString");
			}
			expression = 
				new StringLiteral(ast.getText(), _types.getType("string"));
			expression.setIsConstant(true);
			break;
		case SNEEqlParserTokenTypes.Attribute:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate Attribute " + ast.getText() +
						" attributes= " + input.getAttributes());
			}

			/* This is the name we will be looking for */
			String searchName = ast.getText();

			String[] parts = searchName.split("[.]");
			assert (parts.length==2);
			if (logger.isTraceEnabled()) {
				logger.trace("Parts: " + parts[0] + " " + parts[1]);
			}

			String eName;
			if (extentNameMappings.containsKey(parts[0])) {

				eName = extentNameMappings.get(parts[0]);

				/* The extent reference exists but there is
				 * no extent name, because it refers to an
				 * entire sub-query. In this case, the fix
				 * is to use the display name */
				if ( eName.isEmpty() ){
					eName = parts[0];
					searchName = eName + "." + parts[1];
				}

			} else {

				/* The provided extent name does not exist. This
				 * should result in an error */
				String msg = 
					"Missing name in FROM-clause for extent \"" +
					parts[0] + "\" found in " + ast.getText() + "||";
				logger.warn(msg);
				throw new ExpressionException(msg);
			}

			attributes = input.getAttributes();
			boolean attrFound = false;
			for (int i = 0; i< attributes.size(); i++) {

				Attribute attribute = attributes.get(i);
				if (logger.isTraceEnabled()) {
					logger.trace("Attribute: " + attribute);
				}

				/* The name of the attribute I'm going to check against */
				String attrName = attribute.getAttributeDisplayName();

				/* Now, check if the requested item has been found */
				if ( attrName.equalsIgnoreCase(searchName) ) {

					Attribute newAttribute = copyAttribute(attribute);

					newAttribute.setAttributeDisplayName(searchName);
					expression = newAttribute;
					attrFound = true;
					break;
				}

			}
			if (!attrFound) {
				String msg = "Unable to find Attribute " + 
					ast.getText();
				logger.warn(msg);
				throw new ExpressionException(msg);
			}
			break;
		case SNEEqlParserTokenTypes.Identifier:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate Identifier: " + ast.getText());
			}

			attributes = input.getAttributes();
			int found = -1;
			for (int i = 0; i< attributes.size(); i++) {
				String attrSchemaName = 
					attributes.get(i).getAttributeSchemaName();
				if (attrSchemaName.equalsIgnoreCase(ast.getText())) {
					if (found == -1) {
						found = i;
					} else {
						String msg = "Ambigious reference to " +
								"unqualifeied attribute " +
								ast.getText();
						logger.warn(msg);
						throw new ExpressionException(msg);
					}
				}
			}
			if (found != -1) {

				/* Do not store the actual attribute but rather
				 * copy it and assign that one. Otherwise, there
				 * will be problems */
				Attribute attribute = attributes.get(found);
				Attribute newAttribute = copyAttribute(attribute);
				
				expression = newAttribute;
				break;
			} else {
				String msg = "Unable to find unqualified " +
						"attribute " + ast.getText();
				logger.warn(msg);
				throw new ExpressionException(msg);
			}
		case SNEEqlParserTokenTypes.FUNCTION_NAME:
			if (logger.isTraceEnabled()) {
				logger.trace("Translate FUNCTION_NAME");
			}
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
			if (logger.isTraceEnabled()) {
				logger.trace("Translate boolean operator");
			}
			expressions = new Expression[ast.getNumberOfChildren()];
			count = 0;
			child = ast.getFirstChild();
			while (child != null) {
				expressions[count] = translateExpression(child, input);
				count++;
				child = child.getNextSibling();
			}
			expression = 
				new MultiExpression(expressions, getMultiType(ast),
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
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN translateExpression() " + expression);
		}
		return expression;
	}

	private Attribute copyAttribute(Attribute attribute)
			throws SchemaMetadataException {
		Attribute newAttribute;
		if (attribute instanceof EvalTimeAttribute) {
			newAttribute = new EvalTimeAttribute(attribute);
		} else if (attribute instanceof TimeAttribute) {
			newAttribute = new TimeAttribute(attribute);
		} else if (attribute instanceof IDAttribute) {
			newAttribute = new IDAttribute(attribute);
		} else {
			newAttribute = new DataAttribute(attribute);						
		}
		return newAttribute;
	}

	private Expression getFunction(AST ast, LogicalOperator input) 
	throws ExpressionException, OptimizationException, 
	TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getFunction() " + ast + " " + input);
		}
		assert(ast.getNumberOfChildren() == 1);
		assert(ast.getType() == SNEEqlParserTokenTypes.FUNCTION_NAME);
		Expression inner = 
			translateExpression(ast.getFirstChild(), input);
		Expression expression;
		if ((ast.getText().equalsIgnoreCase("avg")) || 
				(ast.getText().equalsIgnoreCase("average"))) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate average");
			}
			expression = new AggregationExpression(inner, 
					AggregationFunction.AVG, 
					_types.getType("float"));
		} else if (ast.getText().equalsIgnoreCase("count")) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate count");
			}
			/* Count is DEFINITELY integer */
			expression = new AggregationExpression(inner, 
					AggregationFunction.COUNT,
					_types.getType("integer"));
		} else if ((ast.getText().equalsIgnoreCase("minimum")) || 
				(ast.getText().equalsIgnoreCase("min"))) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate minimum");
			}
			expression = new AggregationExpression(inner, 
					AggregationFunction.MIN,
					inner.getType());
		} else if (ast.getText().equalsIgnoreCase("stdev")) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate standard deviation");
			}
			expression = new AggregationExpression(inner,
					AggregationFunction.STDEV, 
					_types.getType("float"));
		} else if ((ast.getText().equalsIgnoreCase("max")) ||
				(ast.getText().equalsIgnoreCase("maximum"))) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate maximum");
			}
			expression = new AggregationExpression(inner,
					AggregationFunction.MAX, 
					inner.getType());
		} else if ((ast.getText().equalsIgnoreCase("sqr")) ||
				(ast.getText().equalsIgnoreCase("square"))) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate square");
			}
			expression = new MultiExpression(
					new Expression[] {inner,inner}, 
					MultiType.MULTIPLY, 
					inner.getType());
		} else if ((ast.getText().equalsIgnoreCase("sqrt")) || 
				(ast.getText().equalsIgnoreCase("squareroot"))) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate square root");
			}
			expression = new MultiExpression(
					new Expression[] {inner}, 
					MultiType.SQUAREROOT, 
					_types.getType("float"));
		} else if (ast.getText().equalsIgnoreCase("sum")) {
			if (logger.isTraceEnabled()) {
				logger.trace("Translate sum");
			}
			expression = new AggregationExpression(inner, 
					AggregationFunction.SUM, 
//					inner.getType());
					_types.getType("float")); 
			//because in nesC floats have a much greater range than 16-bit ints
			//TODO: Make this into a long int type
		} else { 
			String message = "Unprogrammed Function name " +
					"AST Text:" + ast.getText();
			logger.warn(message);
			throw new OptimizationException(message);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getFunction() " + expression);
		}
		return expression;
	}

	private MultiType getMultiType(AST ast) 
	throws OptimizationException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getMultiType() with " + 
					ast.getType() + " " + ast.getText());
		}
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
			if (ast.getText().equals("=")) {
				multiType = MultiType.EQUALS;
			} else if (ast.getText().equals("<")) {
				multiType = MultiType.LESSTHAN;
			} else if (ast.getText().equals(">")) {
				multiType = MultiType.GREATERTHAN;
			} else if (ast.getText().equals(">=")) {
				multiType = MultiType.GREATERTHANEQUALS;
			} else if (ast.getText().equals("<=")) {
				multiType = MultiType.LESSTHANEQUALS;
			} else if (ast.getText().equals("!=")) {
				multiType = MultiType.NOTEQUALS;
			} else {
				String msg = "Unprogrammed PRED AST Text:" + 
					ast.getText();
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
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getMultiType() " + multiType);
		}
		return multiType;
	}

}


