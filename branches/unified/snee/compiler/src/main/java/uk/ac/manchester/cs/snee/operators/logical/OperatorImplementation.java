/****************************************************************************\ 
 *                                                                            *
 *  SNEE (Sensor NEtwork Engine)                                              *
 *  http://code.google.com/p/snee                                             *
 *  Release 1.0, 24 May 2009, under New BSD License.                          *
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
package uk.ac.manchester.cs.snee.operators.logical;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.graph.NodeImplementation;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.NoPredicate;


/**
 * Base class for all operators. Specific operators
 * extend this class.
 * @author Ixent Galpin ,Christian Brenninkmeijerand Steven Lynden
 */
public abstract class OperatorImplementation extends NodeImplementation
implements Operator {

	/**
	 * See java.util.logging.Logger.
	 */
	private Logger logger = 
		Logger.getLogger(OperatorImplementation.class.getName());

	/**
	 * Id to be given to the next Operator to be created.
	 */
	private static int opCount = 0;

	/**
	 * The name of the operator, e.g., ACQUIRE, JOIN.
	 */
	private String operatorName;

	/**
	 * The output data type of the operator,
	 *  e.g., ALLTUPLES, RELATION, STREAM or WINDOW.
	 */
	private OperatorDataType operatorDataType;

//	/**
//	 * The name of the nesC template file.
//	 */
//	private String nesCTemplateName;

	/**
	 * String representation of operator parameters.
	 */
	private String paramStr;

	//	/**
	//	 * The fragment to which this operator belongs.
	//	 */
	//	private Fragment containingFragment = null;

	//IG: TODO:review this
	//CB Appears never to be set.
//	/**
//	 * Partitioning Attributes: The attribute(s) by which data is partitioned.
//	 */
//	private String partitioningAttribute;

	/** 
	 * Predicate that this operator is expected to test data against.
	 */
	private Expression predicate = new NoPredicate();

	protected AttributeType _boolType;

	/**
	 * Makes a clone of the operator without using a new opCount.
	 * @param model Operator to get internal data from.
	 */
	protected OperatorImplementation(Operator model) {
		super(model);
		this.operatorDataType = model.getOperatorDataType();
		this.operatorName = model.getOperatorName();
		//		this.nesCTemplateName = model.getNesCTemplateName();
		this.paramStr = model.getParamStr();
		this.predicate = model.getPredicate();
	}

	/**
	 * Constructs a new operator. 
	 */
	public OperatorImplementation(AttributeType boolType) {
		/* Assign the operator an automatic ID */
		super(new Integer(opCount).toString());
		_boolType = boolType;
		opCount++;        
	}

	/**
	 * Makes a copy of the operator using a new opCount.
	 * @param model Operator to get internal data from.
	 * @param newID boolean flag expected to be true. 
	 */
	protected OperatorImplementation(Operator model, boolean newID) {
		super(new Integer(opCount).toString());
		opCount++;        
		assert (newID);
		this.operatorDataType = model.getOperatorDataType();
		this.operatorName = model.getOperatorName();
		//		this.nesCTemplateName = model.getNesCTemplateName();
		this.paramStr = model.getParamStr();
		this.predicate = model.getPredicate();
	}

	/**
	 * Constructs a new operator, building tree from leaves upwards.
	 * 
	 * @param children The children of an operator
	 */
	// TODO: Change children to arrayList type?
	protected void setChildren(Operator[] children) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setChildren() #children=" + children.length);
		for (Operator element : children) {
			this.addInput(element);
			element.addOutput(this);
		}

		//		//for now, but probably oversimplifying
		//		if (children.length > 0) {
		//			this.partitioningAttribute = children[0].getPartitioningAttribute();
		//		} else {
		//			throw new AssertionError("Unexpected point reached.");
		//		}
		if (logger.isTraceEnabled())
			logger.trace("ENTER setChildren()");
	}

	//	/**
	//	 * @return the fragment to which this operator belongs
	//	 */
	//	public Fragment getContainingFragment() {
	//		return this.containingFragment;
	//	}

	//	/**
	//	 * @return The Fragment Id.
	//	 */
	//	public String getFragID() {
	//		if (this.containingFragment == null) {
	//			return null;
	//		}
	//
	//		return new Integer(this.containingFragment.getID()).toString();
	//	}

	//CB: Used by logicalOptimiser
	/**
	 * Returns the input operator at the index
	 * specified.
	 * @param index Position of the operator to be returned.
	 * @return The child operator with this index.
	 */
	public Operator getInput(int index) {
		return (Operator) super.getInput(index);
	}

	/** 
	 * Gets the operator to which this operator send data.
	 * @param index Position of the operator to be returned.
	 * @return The parent operator with this index.
	 */
	public Operator getOutput(int index) {
		return (Operator) super.getOutput(index);
	}

	//	/** 
	//	 * @return The partitioning Attribute which appears never to be set.
	//	 */
	//	public String getPartitioningAttribute() {
	//		return this.partitioningAttribute;
	//	}

	//CB: For debuging prints out info in this operator and its children
	//CB: IXENT feel free to add output here
	/**
	 * Returns a String representation of the operator. This is used
	 * for debugging.
	 * 
	 * @return A string representation of the operator including its children.
	 */
	public abstract String toString();


	//CB: Used by TreeDisplay and toString for debugging
	//CB: IXENT feel free to add output here
	//CB: Should not include children use to string for that.
	/**
	 * Gets a description of this operator only.
	 * 
	 * @return A string representation of this operator but not its children
	 */
	public String getText() {
		StringBuffer s = new StringBuffer();
		s.append("TYPE: ");
		s.append(this.getOperatorDataType());
		s.append("   OPERATOR: ");
		s.append(this.getOperatorName());
		if (this.getParamStr() != null) {
			s.append(" (");
			s.append(this.getParamStr());
			s.append(" )");
		}
		s.append(" - card: ");
		s.append(new Long(this.getCardinality(CardinalityType.MAX)).toString());
		s.append("\n");
		return s.toString();
	}

	/**
	 * @return The output operator at index 0.
	 */
	public Operator getParent() {
		return this.getOutput(0);
	}

	//	/** 
	//	 * Detects if this operator is the root of a fragment.
	//	 * @return True if this operator is the root of a fragment.
	//	 */
	//	public boolean isFragmentRoot() {
	//		if (this instanceof DeliverOperator) {
	//			return true;
	//		}
	//		if (this.getParent() instanceof ExchangeOperator) {
	//			return true;
	//		}
	//
	//		return false;
	//	}

	//	/**
	//	 * Sets the containing fragment of this operator.
	//	 * @param f the containing fragment
	//	 */
	//	public void setContainingFragment(Fragment f) {
	//		if (!(this instanceof ExchangeOperator)) {
	//			this.containingFragment = f;
	//		} else {
	//			System.err.println("Exchange operator cannot belong to fragment");
	//		}
	//	}

	/**
	 * Iterator to traverse the immediate children of the current operator.
	 * @return An iterator over the children
	 */
	public Iterator<Operator> childOperatorIterator() {

		List<Operator> opList = new ArrayList<Operator>();

		for (int n = 0; n < this.getInDegree(); n++) {
			Operator op = this.getInput(n);
			opList.add(op);
		}

		return opList.iterator();
	}    

//	/**
//	 * Merges two arrays of int (or sites).
//	 * With duplicates removed.
//	 * @param bigger The larger of the two arrays 
//	 * @param smaller The smaller of the two arrays.
//	 * @return A single array with all values of both 
//	 * sorted and without duplicates.
//	 */
//	private int[] mergeIntArrays(int[] bigger, int[]smaller) {
//		if (smaller.length > bigger.length) {
//			return this.mergeIntArrays(smaller, bigger);
//		}
//		int size = bigger.length + smaller.length;
//		int i;
//		for (i = 0; i < smaller.length; i++) {
//			if (Arrays.binarySearch(bigger, smaller[i]) >= 0) {
//				size--;
//			}
//		}
//		if (size != bigger.length) {
//			int[] result = new int[size];
//			for (i = 0; i < bigger.length; i++) {
//				result[i] = bigger[i];
//			}
//			for (int element : smaller) {
//				if (Arrays.binarySearch(bigger, element) < 0) {
//					result[i] = element;
//					i++;
//				}
//			}
//			this.logger.trace("result = " + Arrays.toString(result));
//			Arrays.sort(result);
//			return result;
//		} else {
//			return bigger;
//		}
//	}    

	//	/**
	//	 * Get the source node for the operator by getting the source nodes of
	//	 * it children.
	//	 * 
	//	 * @return Sorted list of nodes that provide data for this operator.
	//	 */
	//	public int[] defaultGetSourceSites() {
	//		Iterator<Operator> ops = this.childOperatorIterator();
	//		int[] result = ops.next().getSourceSites();
	//		while (ops.hasNext()) {
	//			result =  this.mergeIntArrays(result, ops.next().getSourceSites());
	//		}
	//		return result;
	//	}

	//	/** {@inheritDoc} */
	//	public void setPredicates(Collection<Predicate> predicates) {
	//		assert (acceptsPredicates());
	//		ArrayList<Attribute> attributes = this.getAttributes();
	//		Predicate[] predicateArray = new Predicate[predicates.size()];
	//		predicateArray = predicates.toArray(predicateArray); 
	//		setPredicate(ExpressionConvertor.convert(predicateArray, attributes));
	//	}

	//	/**
	//	 * Obtains the input cardinality from all incoming sites.
	//	 * For exchanges this is the producer cardinality
	//	 * @param card Type of cardinality to be used.
	//	 * @param node Site for which incoming should be calculated
	//	 * @param daf DAF this operator is part of
	//	 * @param index Index of incoming operator. (0 unless a join)
	//	 * @return The total physicalMaxCardinality for all sites 
	//	 * that feed into node.
	//	 */
	//	public int getInputCardinality(CardinalityType card, 
	//			Site node, DAF daf, int index) {
	//		int total = 0;
	//		Operator inputOp = this.getInput(index);
	//		Iterator<Site> inputs;
	//
	//		inputs = daf.getInputOperatorInstanceSites(this, node, index);
	//		while (inputs.hasNext()) {
	//			total = total
	//			+ inputOp.getCardinality(card, inputs.next(), daf);
	//		}
	//		return total;
	//	}

	//	/**
	//	 * Obtains the input cardinality from one incoming sites.
	//	 * For exchanges this is the producer cardinality
	//	 *
	//	 * WARNING: ASSUMES THAT ALL INSTANCES ARE DISTRIBUTED EVENLY!
	//	 * 
	//	 * @param card Type of Cardinality to be used to calculate cost.
	//	 * @param index Index of incoming operator. (0 unless a join)
	//	 * @param numberOfInstances Number of instances if this operator in the query plan.
	//	 * @return The cardinality for all sites that feed into one node.
	//	 */
	//	public int getInputCardinality(CardinalityType card, 
	//			int index, int numberOfInstances) {
	//		Operator inputOp = this.getInput(index);
	//		return inputOp.getCardinality(card)/numberOfInstances;
	//	}

	//	/** 
	//	 * Provides the general overhead of running an operator.
	//	 * Only includes cost that are the same for all operators, 
	//	 * and any number of tuples (zero or more).
	//	 * 
	//	 * Current includes cost off calling method and signaling result ready. 
	//	 * 
	//	 * @return Overhead costs.
	//	 */
	//	protected double getOverheadTimeCost() {
	//		return CostParameters.getCallMethod()
	//		+ CostParameters.getSignalEvent();
	//	}

	//	/**
	//	 * Obtains the input cardinality from all incoming sites.
	//	 * For exchanges this is the producer cardinality
	//	 * @param card Type of cardinality to be used.
	//	 * @param node Site for which incoming should be calculated
	//	 * @param daf DAF this operator is part of
	//	 * @param round Defines if rounding reserves should be included or not
	//	 * @param index Index of incoming operator. (0 unless a join)
	//	 * @return The total physicalMaxCardinality for all sites 
	//	 * that feed into node.
	//	 */
	//	public AlphaBetaExpression getInputCardinality(
	//			CardinalityType card,	Site node, DAF daf, 
	//			boolean round, int index) {
	//		AlphaBetaExpression total = new AlphaBetaExpression();
	//		Operator inputOp = this.getInput(index);
	//		Iterator<Site> inputs;
	//
	//		inputs = daf.getInputOperatorInstanceSites(this, node, index);
	//		while (inputs.hasNext()) {
	//			total.add(inputOp.getCardinality(card, inputs.next(), daf, round));
	//		}
	//		return total;
	//	}

	//	/**
	//	 * The physical size of the tuple including any control information.
	//	 * 
	//	 * May takes into consideration that an attribute of size X
	//	 * must start at a location where location mod x = 0.
	//	 * The total size must also be a multiple of the largest attribute.
	//	 * 
	//	 * @return The size of the tuple
	//	 */
	//	public int getPhysicalTupleSize() {
	//		//Tossim needs the extra space to send packets
	//		if (Settings.CODE_GENERATION_TARGETS.contains(CodeGenTarget.TOSSIM)) {
	//			return getRoundedPhysicalTupleSize();
	//		}
	//		if (Settings.CODE_GENERATION_TARGETS.contains(CodeGenTarget.TOSSIM2)) {
	//			return getRoundedPhysicalTupleSize();
	//		}
	//		//CB: Not sure what INSENSE does so lets be save. 
	//		if (Settings.CODE_GENERATION_TARGETS.contains(CodeGenTarget.INSENSE)) {
	//			return getRoundedPhysicalTupleSize();
	//		}
	//		//Avrora does not need the extra space.
	//		return getPurePhysicalTupleSize();
	//	}    

	//	/**
	//	 * The physical size of the tuple including any control information.
	//	 * 
	//	 * This takes into consideration that an attribute of size X
	//	 * must start at a location where location mod x = 0.
	//	 * The total size must also be a multiple of the largest attribute.
	//	 * 
	//	 * @return The size of the tuple
	//	 */
	//	public int getRoundedPhysicalTupleSize() {
	//		ArrayList<Attribute> attributes = this.getAttributes();
	//		int totalSize = 0;
	//		int blockSize = 0;
	//		for (int i = 0; i < attributes.size(); i++) {
	//			int size = attributes.get(i).getType().getSize();
	//			//keep track of largest object
	//			if (size > blockSize) {
	//				blockSize = size;
	//			}
	//			//make sure object starts on a multiple of itself
	//			if ((totalSize % size) > 0) {
	//				totalSize = totalSize + size - (totalSize % size);
	//			}
	//			//add object
	//			totalSize = totalSize + size;
	//		}
	//		//fill to a multiple of the size of largest object
	//		if ((totalSize % blockSize) > 0) {
	//			totalSize = totalSize + blockSize - (totalSize % blockSize);
	//		}
	//		return totalSize;
	//	}    

	//	/**
	//	 * The physical size of the tuple including any control information.
	//	 * 
	//	 * Does NOT takes into consideration that an attribute of size X
	//	 * must start at a location where location mod x = 0.
	//	 * The total size need not be a multiple of the largest attribute.
	//	 * 
	//	 * @return The size of the tuple
	//	 */
	//	public int getPurePhysicalTupleSize() {
	//		ArrayList<Attribute> attributes = this.getAttributes();
	//		int totalSize = 0;
	//		for (int i = 0; i < attributes.size(); i++) {
	//			int size = attributes.get(i).getType().getSize();
	//			totalSize = totalSize + size;
	//		}
	//		return totalSize;
	//	}    

//	/** {@inheritDoc} */
//	public abstract OperatorImplementation shallowClone();

	//	/** {@inheritDoc} */
	//	public String getNesCTemplateName() {
	//		return this.nesCTemplateName;
	//	}

	//	/**
	//	 * @param newNesCTemplateName new value for nesCTemplateName 
	//	 */
	//	protected void setNesCTemplateName(
	//			String newNesCTemplateName) {
	//		this.nesCTemplateName = newNesCTemplateName;
	//	}

	/** {@inheritDoc} */
	public OperatorDataType getOperatorDataType() {
		assert (operatorDataType != null);
		return this.operatorDataType;
	}

	/**
	 * @param newOperatorDataType New value.
	 */
	protected void setOperatorDataType(
			OperatorDataType newOperatorDataType) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setOperatorDataType()");
		assert (newOperatorDataType != null);
		this.operatorDataType = newOperatorDataType;
		if (logger.isTraceEnabled())
			logger.trace("RETURN setOperatorDataType()");
	}

	/** {@inheritDoc} */    
	public String getOperatorName() {
		return this.operatorName;
	}

	/**
	 * @param newOperatorName new value.
	 */
	protected void setOperatorName(String newOperatorName) {
		this.operatorName = newOperatorName;
	}

	/** {@inheritDoc} */    
	public String getParamStr() {
		return this.paramStr;
	}

	/**
	 * @param newParamStr new Value
	 */
	protected void setParamStr(String newParamStr) {
		this.paramStr = newParamStr;
	}    

	/** {@inheritDoc} 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException */    
	public String getTupleAttributesStr(int maxPerLine) 
	throws SchemaMetadataException, TypeMappingException {
		List<Attribute> attributes = getAttributes();

		StringBuffer strBuff = new StringBuffer();
		strBuff.append("(");
		for (int i = 0; i < attributes.size(); i++) {
			//commas
			if (i > 0) {
				strBuff.append(", ");
				if ((i % maxPerLine == 0) && (i - 1) < attributes.size()) {  
					strBuff.append("\\n");
				}
			}
			strBuff.append(attributes.get(i));
			strBuff.append(":");
			assert (attributes.get(i) != null);
			assert (attributes.get(i).getType() != null);            
			strBuff.append(Utils.capFirstLetter(
					attributes.get(i).getType().getName()));

		}
		strBuff.append(")");
		return strBuff.toString();
	}

//	/** {@inheritDoc} */    
//	public int defaultGetOutputQueueCardinality(
//			Site node, DAF daf) {
//		return this.getCardinality(CardinalityType.PHYSICAL_MAX, node, daf);
//	}

//	/** {@inheritDoc} */    
//	public int defaultGetOutputQueueCardinality(int numberOfInstances) {
//		return this.getCardinality(CardinalityType.PHYSICAL_MAX) / numberOfInstances;
//	}

	/** 
	 * List of the attribute returned by this operator.
	 * 
	 * @return List of the returned attributes.
	 */
	public List<Attribute> defaultGetAttributes() {
		return (this.getInput(0)).getAttributes();		
	}

	/** {@inheritDoc} */    
	public List<Expression> defaultGetExpressions() {
		List<Expression> expressions = new ArrayList<Expression>(); 
		expressions.addAll(getAttributes());
		return expressions;
	}

//	/** 
//	 * Calculates the physical size of the state of this operator.
//	 * 
//	 * Default implmentation assumes the cost is 
//	 * the physical tuple size multiplied by the size of the output queue.
//	 * 
//	 * Does not included the size of the input 
//	 * as these are assumed passed by reference.
//	 *
//	 * Does not include the size of the code itself.
//	 * 
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public int defaultGetDataMemoryCost(Site node, DAF daf) {
//		logger.trace("Memory for OP"+ this);
//		int size = this.getPhysicalTupleSize();
//		logger.trace("Tuple Size: " + size);
//		int card = this.getOutputQueueCardinality(node, daf);
//		logger.trace("OutputQueueCardinality = "+card);
//		int memoryCost = size * card;
//		logger.trace("Memory Cost = "+memoryCost);
//		return memoryCost;
//	}

//	/** 
//	 * Calculates the physical size of the state of this operator.
//	 * 
//	 * Does not included the size of the input 
//	 * as these are assumed passed by reference.
//	 *
//	 * Does not include the size of the code itself.
//	 * 
//	 * WARNING: ASSUMES THAT ALL INSTANCES ARE DISTRIBUTED EVENLY!
//	 * 
//	 * @param numberOfInstances Number of instances if this operator in the query plan.
//	 * Unless numberOfInstance = number of source sites or 1 the correctness 
//	 * 	of this method depends on instances being perfectly distributed.
//	 * @return Estimated number of bytes of RAM used by this operator.
//	 */
//	public int getDataMemoryCost(int numberOfInstances){
//		logger.trace("Memory for OP"+ this);
//		int size = this.getPhysicalTupleSize();
//		logger.trace("Tuple Size: " + size);
//		int card = this.getOutputQueueCardinality(numberOfInstances);
//		logger.trace("OutputQueueCardinality = "+card);
//		int memoryCost = size * card;
//		logger.trace("Memory Cost = "+memoryCost);
//		return memoryCost;
//	}


	/** {@inheritDoc} */    
	public Expression getPredicate() {
		return predicate;
	}

	/** {@inheritDoc} 
	 * @throws AssertionError 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException */    
	public void setPredicate(Expression newPredicate) 
	throws SchemaMetadataException, AssertionError, TypeMappingException 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER setPredicate() with " + newPredicate);
		if (newPredicate instanceof NoPredicate) {
			if (logger.isTraceEnabled())
				logger.trace("Instance of NoPredicate");
			this.predicate = newPredicate;
			this.paramStr = paramStr + predicate.toString();
			if (logger.isDebugEnabled())
				logger.debug("RETURN setPredicate()");
			return;
		}
		if (logger.isTraceEnabled())
			logger.trace("Predicate expression type: " + 
					newPredicate.getType() + 
					"\n\t\t\tBoolean type: " + _boolType);
		if (newPredicate.getType() != _boolType) {
			String msg = "Illegal attempt to use a none boolean " +
			"expression as a predicate.";
			logger.warn(msg);
			throw new AssertionError(msg);
		}
		if (this.acceptsPredicates()) {
			this.predicate = newPredicate;
			this.paramStr = paramStr + predicate.toString();
		} else {
			String msg = "Illegal call.";
			logger.warn(msg);
			throw new AssertionError(msg);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN setPredicate()");
	}

//	/**
//	 * Displays the results of the cost functions.
//	 * Stub returns -1 until developed..
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//	 */
//	public double getTimeCost2(Site node, DAF daf) {
//		throw new AssertionError ("stub code reached. in "+this.toString());
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * Stub returns -1 until developed..
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return the calculated time
//	 */
//	public double getTimeCost2(Site node, DAF daf, long bufferingFactor) {
//		return getTimeCost2(node, daf);
//	}

//	/** {@inheritDoc} */    
//	public double getEnergyCost(CardinalityType card, int numberOfInstances) {
//		double milliSeconds = getTimeCost(card, numberOfInstances);
//		double marginal = AvroraCostParameters.CPUACTIVEAMPERE - AvroraCostParameters.CPUPOWERSAVEAMPERE;
//		return milliSeconds * marginal;
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public double getEnergyCost2(Site node, DAF daf){
//		throw new AssertionError ("stub code reached.");
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public double getEnergyCost2(Site node, DAF daf, long bufferingFactor) {
//		return getEnergyCost2(node, daf);
//	}

//	/**
//	 * Displays the results of the cost functions.
//	 * Underlying Methods are still under development.
//	 * @param node Physical mote on which this operator has been placed.
//	 * @param daf Distributed query plan this operator is part of.
//	 * @return OutputQueueCardinality * PhytsicalTuplesSize
//	 */
//	public int getDataMemoryCost2(Site node, DAF daf) {
//		return -1;
//	}

}

