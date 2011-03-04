package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.NodeImplementation;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Expression;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.JoinOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperatorImpl;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;
import uk.ac.manchester.cs.snee.operators.logical.RStreamOperator;
import uk.ac.manchester.cs.snee.operators.logical.SelectOperator;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;

public abstract class SensornetOperatorImpl extends NodeImplementation implements
		SensornetOperator {

	/**
	 * Logger for this class.
	 */
	protected Logger logger = Logger.getLogger(this.getClass().getName());
	
	/**
	 * The name of the operator, e.g., ACQUIRE, JOIN.
	 */
	private String operatorName;
	
	/**
	 * Id to be given to the next Operator to be created.
	 */
	private static int opCount = 0;
	
	//XXX: Do we need this if we are inheriting?
	/**
	 * The logical operator associated with this physical operator.
	 */
	private LogicalOperator logicalOp;

	/**
	 * Fragment that this operator is in.
	 */
	private Fragment containingFragment = null;	
	
	/**
	 * The name of the nesC template file.
	 */
	private String nesCTemplateName;
	
	/**
	 * Cost parameters.
	 */
	protected CostParameters costParams;
	
	public SensornetOperatorImpl(LogicalOperator op, CostParameters costParams) 
	throws SNEEException, SchemaMetadataException {
		this(op, costParams, true);
	}

	public SensornetOperatorImpl(LogicalOperator op, CostParameters costParams, boolean instantiateChildren) 
	throws SNEEException, SchemaMetadataException
	{
		super(new Integer(opCount).toString());
		opCount++;
		
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetOperatorImpl() " + op);
		}
		this.logicalOp = op;
		this.costParams = costParams;
		this.setOperatorName("Sensornet"+this.logicalOp.getOperatorName());
		if (op instanceof SensornetOperatorImpl) {
			logger.warn("Physical operator "+op.getID()+"instead of logical "+
					"operator passed to physical operator constructor.");
		}
		
		if (instantiateChildren) {
			// Instantiate the child operator(s)
			int numChildren = op.getInDegree();
			for (int i=0; i<numChildren; i++) {
				LogicalOperator logicalChild = op.getInput(i);
				SensornetOperatorImpl phyChild = (SensornetOperatorImpl) 
					getSensornetOperator(logicalChild, costParams);
				this.setInput(phyChild, i);
				phyChild.setOutput(this, 0);			
			}						
		}
		
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetOperatorImpl() " + op);			
		}		
	}
	
	//Exchange operators only
	public SensornetOperatorImpl(CostParameters costParams) {
		super(new Integer(opCount).toString());
		opCount++;
		
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetOperatorImpl() ");
		}
		this.costParams=costParams;
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetOperatorImpl() ");
		}
	}

	public SensornetOperatorImpl getSensornetOperator(LogicalOperator op,
	CostParameters costParams)
	throws SNEEException, SchemaMetadataException {
		SensornetOperatorImpl phyOp = null;
		if (op instanceof AcquireOperator) {
			phyOp = new SensornetAcquireOperator(op,costParams);
		} else if (op instanceof AggregationOperator) {
			phyOp = new SensornetSingleStepAggregationOperator(op,costParams);
		} else if (op instanceof DeliverOperator) {
			phyOp = new SensornetDeliverOperator(op,costParams);
		} else if (op instanceof JoinOperator) {
			phyOp = new SensornetNestedLoopJoinOperator(op,costParams);
		} else if (op instanceof ProjectOperator) {
			phyOp = new SensornetProjectOperator(op,costParams);
		} else if (op instanceof RStreamOperator) {
			phyOp = new SensornetRStreamOperator(op,costParams);
		} else if (op instanceof SelectOperator) {
			phyOp = new SensornetSelectOperator(op,costParams);
//			} else if (op instanceof UnionOperator) {
//			phyOp = new UnionOperatorImpl(op);
		} else if (op instanceof WindowOperator) {
			phyOp = new SensornetWindowOperator(op,costParams);
//			if (((WindowOperator) op).isTimeScope()) {
//				phyOp = new SensornetTimeWindowOperator(op);
//			} else {
//				phyOp = new SensornetTupleWindowOperator(op);
//			}
		} else {
			String msg = "Unsupported operator " + op.getOperatorName();
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		return phyOp;
	}
	
	/**
	 * @return The output operator at index 0.
	 */
	public SensornetOperator getParent() {
		return (SensornetOperator) this.getOutput(0);
	}
	
	public void setOperatorName(String newName) {
		this.operatorName = newName;
	}
	
	public String getOperatorName() {
		return this.operatorName;
	}
	
	/**
	 * @return the fragment to which this operator belongs
	 */
	public Fragment getContainingFragment() {
		return this.containingFragment;
	}

	/**
	 * @return The Fragment Id.
	 */
	public String getFragID() {
		if (this.containingFragment == null) {
			return null;
		}
		return new Integer(this.containingFragment.getID()).toString();
	}

	/** 
	 * Detects if this operator is the root of a fragment.
	 * @return True if this operator is the root of a fragment.
	 */
	public boolean isFragmentRoot() {
		if (this instanceof SensornetDeliverOperator) {
			return true;
		}
		if (this.getParent() instanceof SensornetExchangeOperator) {
			return true;
		}

		return false;
	}

	/**
	 * Sets the containing fragment of this operator.
	 * @param f the containing fragment
	 */
	public void setContainingFragment(Fragment f) {
		if (!(this instanceof SensornetExchangeOperator)) {
		this.containingFragment = f;
		} else {
			System.err.println("Exchange operator cannot belong to fragment");
		}
	}

	public int getCardinality(CardinalityType cardType) {
		return this.getLogicalOperator().getCardinality(cardType);
	}
	
	public boolean isAttributeSensitive() {
		return this.getLogicalOperator().isAttributeSensitive();
	}


	//	/** {@inheritDoc} */
	//	public void setPredicates(Collection<Predicate> predicates) {
	//		assert (acceptsPredicates());
	//		ArrayList<Attribute> attributes = this.getAttributes();
	//		Predicate[] predicateArray = new Predicate[predicates.size()];
	//		predicateArray = predicates.toArray(predicateArray); 
	//		setPredicate(ExpressionConvertor.convert(predicateArray, attributes));
	//	}

		/**
		 * Obtains the input cardinality from all incoming sites.
		 * For exchanges this is the producer cardinality
		 * @param card Type of cardinality to be used.
		 * @param node Site for which incoming should be calculated
		 * @param daf DAF this operator is part of
		 * @param index Index of incoming operator. (0 unless a join)
		 * @return The total physicalMaxCardinality for all sites 
		 * that feed into node.
		 * @throws OptimizationException 
		 */
		public int getInputCardinality(CardinalityType card, 
				Site node, DAF daf, int index) throws OptimizationException {
			int total = 0;
			SensornetOperator inputOp = (SensornetOperator)this.getInput(index);
			Iterator<Site> inputs;
	
			inputs = daf.getInputOperatorInstanceSites(this, node, index);
			while (inputs.hasNext()) {
				total = total
				+ inputOp.getCardinality(card, inputs.next(), daf);
			}
			return total;
		}

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

		/** 
		 * Provides the general overhead of running an operator.
		 * Only includes cost that are the same for all operators, 
		 * and any number of tuples (zero or more).
		 * 
		 * Current includes cost off calling method and signaling result ready. 
		 * 
		 * @return Overhead costs.
		 */
		protected double getOverheadTimeCost() {
			return costParams.getCallMethod()
			+ costParams.getSignalEvent();
		}

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

		//XXX: Why are there two getPhysicalTupleSize emthods?
		   /**
	     * The physical size of the tuple including any control information.
	     * 
	     * This takes into consideration that an attribute of size X
	     * must start at a location where location mod x = 0.
	     * The total size must also be a multiple of the largest attribute.
	     * 
	     * @return The size of the tuple
		 * @throws TypeMappingException 
		 * @throws SchemaMetadataException 
	     */
	    public final int getPhysicalTupleSize() throws SchemaMetadataException, TypeMappingException {
			if (this instanceof SensornetExchangeOperator) {
				return this.getLeftChild().getPhysicalTupleSize();
			}
	    	
	        final List<Attribute> attributes = this.logicalOp.getAttributes();
	        int totalSize = 0;
	        int blockSize = 0;
	        for (int i = 0; i < attributes.size(); i++) {
	            final int size = attributes.get(i).getType().getSize();
	            //keep track of largest object
	            if (size > blockSize) {
	                blockSize = size;
	            }
	            //make sure object starts on a multiple of itself
	            if ((totalSize % size) > 0) {
	                totalSize = totalSize + size - (totalSize % size);
	            }
	            //add object
	            totalSize = totalSize + size;
	        }
	        //fill to a multiple of the size of largest object
	        if ((totalSize % blockSize) > 0) {
	            totalSize = totalSize + blockSize - (totalSize % blockSize);
	        }
	        return totalSize;
	    }    
		
//		/**
//		 * The physical size of the tuple including any control information.
//		 * 
//		 * May takes into consideration that an attribute of size X
//		 * must start at a location where location mod x = 0.
//		 * The total size must also be a multiple of the largest attribute.
//		 * 
//		 * @return The size of the tuple
//		 * @throws TypeMappingException 
//		 * @throws SchemaMetadataException 
//		 */
//		public int getPhysicalTupleSize() 
//		throws SchemaMetadataException, TypeMappingException {
////			//Tossim needs the extra space to send packets
////			if (Settings.CODE_GENERATION_TARGETS.contains(CodeGenTarget.TOSSIM)) {
////				return getRoundedPhysicalTupleSize();
////			}
////			if (Settings.CODE_GENERATION_TARGETS.contains(CodeGenTarget.TOSSIM2)) {
////				return getRoundedPhysicalTupleSize();
////			}
//			//Avrora does not need the extra space.
//			return getPurePhysicalTupleSize();
//		}    

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

	/**
	 * The physical size of the tuple including any control information.
	 * 
	 * Does NOT takes into consideration that an attribute of size X
	 * must start at a location where location mod x = 0.
	 * The total size need not be a multiple of the largest attribute.
	 * 
	 * @return The size of the tuple
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	private int getPurePhysicalTupleSize() 
	throws SchemaMetadataException, TypeMappingException {
		List<Attribute> attributes = this.getLogicalOperator().getAttributes();
		int totalSize = 0;
		for (int i = 0; i < attributes.size(); i++) {
			int size = attributes.get(i).getType().getSize();
			totalSize = totalSize + size;
		}
		return totalSize;
	}    

	/** {@inheritDoc} 
	 * @throws OptimizationException */    
	public int defaultGetOutputQueueCardinality(
			Site node, DAF daf) throws OptimizationException {
		return this.getCardinality(CardinalityType.PHYSICAL_MAX, node, daf);
	}

//	/** {@inheritDoc} */    
//	public int defaultGetOutputQueueCardinality(int numberOfInstances) {
//		return this.getCardinality(CardinalityType.PHYSICAL_MAX) / numberOfInstances;
//	}

	/** 
	 * Calculates the physical size of the state of this operator.
	 * 
	 * Default implmentation assumes the cost is 
	 * the physical tuple size multiplied by the size of the output queue.
	 * 
	 * Does not included the size of the input 
	 * as these are assumed passed by reference.
	 *
	 * Does not include the size of the code itself.
	 * 
	 * @param node Physical mote on which this operator has been placed.
	 * @param daf Distributed query plan this operator is part of.
	 * @return OutputQueueCardinality * PhytsicalTuplesSize
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws OptimizationException 
	 */
	public int defaultGetDataMemoryCost(Site node, DAF daf) 
	throws SchemaMetadataException, TypeMappingException, OptimizationException {
		logger.trace("Memory for OP"+ this);
		int size = this.getPhysicalTupleSize();
		logger.trace("Tuple Size: " + size);
		int card = this.getOutputQueueCardinality(node, daf);
		logger.trace("OutputQueueCardinality = "+card);
		int memoryCost = size * card;
		logger.trace("Memory Cost = "+memoryCost);
		return memoryCost;
	}

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

	public String getNesCTemplateName() {
		return this.nesCTemplateName;
	}

	protected void setNesCTemplateName(String nescTemplateName) {
		this.nesCTemplateName = nescTemplateName;
	}
	
	public LogicalOperator getLogicalOperator() {
		return this.logicalOp;
	}
	
    /**
     * Merges two arrays of int (or sites).
     * With duplicates removed.
     * @param bigger The larger of the two arrays 
     * @param smaller The smaller of the two arrays.
     * @return A single array with all values of both 
     * sorted and without duplicates.
     */
    private int[] mergeIntArrays(final int[] bigger, final int[]smaller) {
        if (smaller.length > bigger.length) {
            return this.mergeIntArrays(smaller, bigger);
        }
        int size = bigger.length + smaller.length;
        int i;
        for (i = 0; i < smaller.length; i++) {
            if (Arrays.binarySearch(bigger, smaller[i]) >= 0) {
                size--;
            }
        }
        if (size != bigger.length) {
            final int[] result = new int[size];
            for (i = 0; i < bigger.length; i++) {
                result[i] = bigger[i];
            }
            for (int element : smaller) {
                if (Arrays.binarySearch(bigger, element) < 0) {
                    result[i] = element;
                    i++;
                }
            }
            this.logger.trace("result = " + Arrays.toString(result));
            Arrays.sort(result);
            return result;
        } else {
            return bigger;
        }
    }    
	
    /**
     * Get the source node for the operator by getting the source nodes of
     * it children.
     * 
     * @return Sorted list of nodes that provide data for this operator.
     */
    public final int[] defaultGetSourceSites() {
        final Iterator<SensornetOperator> ops = this.childOperatorIterator();
        int[] result = ops.next().getSourceSites();
        while (ops.hasNext()) {
            result =  this.mergeIntArrays(result, ops.next().getSourceSites());
        }
        return result;
    }

	/**
	 * Iterator to traverse the immediate children of the current operator.
	 * @return An iterator over the children
	 */
	public Iterator<SensornetOperator> childOperatorIterator() {

		List<SensornetOperator> opList = new ArrayList<SensornetOperator>();
		for (int n = 0; n < this.getInDegree(); n++) {
			SensornetOperator op = (SensornetOperator) this.getInput(n);
			opList.add(op);
		}
		return opList.iterator();
	}	
	
	//delegate
	public SensornetOperator getLeftChild() {
		return (SensornetOperator)this.getInput(0);
	}
	
	//delegate
	public SensornetOperator getRightChild() {
		return (SensornetOperator)this.getInput(1);
	}
	
	//delegate except for exchange operators or incremental aggregates
	public List<Attribute> getAttributes() {
		return this.getLeftChild().getAttributes();
	}

	//delegate
	public List<Expression> getExpressions() {
		return this.getLogicalOperator().getExpressions();	
	}

	//delegate
	public boolean isLocationSensitive() {
		return this.getLogicalOperator().isLocationSensitive();
	}
	
	//delegate
	public boolean isRecursive() {
		return false;
	}

	//delegate
	public String getTupleAttributesStr(int maxPerLine) throws SchemaMetadataException, TypeMappingException {
		return this.getLogicalOperator().getTupleAttributesStr(maxPerLine);
	}
	
}
