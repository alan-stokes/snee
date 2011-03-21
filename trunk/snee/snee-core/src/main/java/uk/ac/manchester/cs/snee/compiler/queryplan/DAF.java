package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

/**
 * The Sensor Network Fragmented-algebraic form of the query plan operator tree.
 */
public class DAF extends SNEEAlgebraicForm {

    /**
     * Logger for this class.
     */
    private static  Logger logger = Logger.getLogger(DAF.class.getName());
	
	/**
	 * The physical-algebraic form of the query plan operator tree from which
	 * DAF is derived.
	 */
	private PAF paf;
	
	/**
	 * The routing tree from which DAF is derived.
	 */
	private RT rt;
	
    /**
     *  The root fragment in the query plan. 
     *  Determined during partitioning
     * 	 
     */
    private Fragment rootFragment;

	/**
	 * The tree of Physical operators
	 */
	private Tree physicalOperatorTree;
    
    /** 
     *  Set of fragments in the query plan.
     *  Determined during partitioning
     *  
     */
    private final HashSet<Fragment> fragments = new HashSet<Fragment>();	
    
    /**
     * Counter to assign unique id to different candidates.
     */
    protected static int candidateCount = 0;
    
	/**
	 * Constructor for Physical-algebraic form.
	 * @param deliverPhyOp the physical operator which is the root of the tree
	 * @param dlaf The distributed logical-algebraic form of the query plan 
	 * operator tree from which PAF is derived.
	 * @param queryName The name of the query
	 * @throws SNEEException 
	 */
	public DAF(final PAF paf, final RT rt, final String queryName) 
	throws SNEEException, SchemaMetadataException {
		super(queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER DAF()"); 
		this.paf=paf;
		this.rt=rt;
		//XXX: No copying for now.
		this.physicalOperatorTree = paf.getOperatorTree();
		Fragment.resetFragmentCounter(); 
		this.rootFragment = new Fragment();		
		this.fragments.add(this.rootFragment);
		if (logger.isDebugEnabled())
			logger.debug("RETURN DAF()"); 	
	}

    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }

    /**
     * Gets the PAF that this DAF is associated with.
     * @return
     */
	public PAF getPAF() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getPAF()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getPAF()"); 
		return this.paf;
	}
	
    /**
     * Gets the RT that this DAF is associated with.
     * @return
     */
	public RT getRT() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getRT()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getRT()"); 
		return this.rt;
	}	

	 /** {@inheritDoc} */
	protected String generateID(String queryName) {
//		if (logger.isDebugEnabled())
//			logger.debug("ENTER generateID()"); 
    	candidateCount++;
//		if (logger.isDebugEnabled())
//			logger.debug("RETURN generateID()"); 
    	return queryName + "-DAF-" + candidateCount;	
    }

	 /** {@inheritDoc} */
	public String getDescendantsString() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDescendantsString()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDescendantsString()"); 
		return this.getID()+"-"+this.paf.getDescendantsString();
	}

	
	/**
	 * Generates an iterator to traverse operator tree.
	 * @param order
	 * @return
	 */
	public Iterator<SensornetOperator> operatorIterator(TraversalOrder order) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER operatorIterator()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN operatorIterator()"); 
		return this.physicalOperatorTree.nodeIterator(order);
	}

	/**
	 * Returns the physical operator tree.
	 * @return
	 */
	public Tree getOperatorTree() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getOperatorTree()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getOperatorTree()"); 
		return this.physicalOperatorTree;
	}
	
	/**
	 * Returns the fragments in the query plan.
	 * @return the fragments in the query plan.
	 */
	public final HashSet<Fragment> getFragments() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getFragments()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getFragments()"); 
		return fragments;
	}

    /**
     * Returns the root fragment of the query plan.
     * @return the root fragment
     */
	public final Fragment getRootFragment() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getRootFragment()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getRootFragment()"); 
		return rootFragment;
	}
	
    /**
     * Returns the fragment with the specified ID.
     * @param fragID the identifier of the desired fragment
     * @return the fragment the fragment requested
     */
    public final Fragment getFragment(final String fragID) {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER getFragment()"); 
		final Iterator<Fragment> fragIter = this.fragments.iterator();
		while (fragIter.hasNext()) {
		    final Fragment frag = fragIter.next();
		    if (frag.getID().equals(fragID)) {
		    	if (logger.isDebugEnabled())
					logger.debug("RETURN getFragment()"); 
		    	return frag;
		    }
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getFragment()"); 
		return null;
    }	
    
    /**
     * Helper method to recursively get the leaf fragments in the FAF/DAF.
     * @param frag			the current fragment being visited
     * @param leafFragments the leaf fragments of the query plan
     * @return the leaf fragments in the query plan
     */
    private HashSet<Fragment> getLeafFragments(final Fragment frag,
    	    final HashSet<Fragment> leafFragments) {
    	if (logger.isTraceEnabled())
			logger.trace("ENTER getLeafFragments()"); 
    	if (frag.isLeaf()) {
    	    leafFragments.add(frag);
    	}
    	//fragment traversal
    	for (int fragChildIndex = 0; fragChildIndex < frag
    		.getNumChildExchangeOperators(); fragChildIndex++) {
    	    final SensornetExchangeOperator c = frag
    		    .getChildExchangeOperator(fragChildIndex);
    	    this.getLeafFragments(c.getSourceFragment(), leafFragments);
    	}
    	if (logger.isTraceEnabled())
			logger.trace("RETURN getLeafFragments()"); 
    	return leafFragments;
    }

    /**
     * Gets the leaf fragments in the FAF/DAF.
     * @return the leaf fragments
     */
    public final HashSet<Fragment> getLeafFragments() {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER getLeafFragments()"); 
    	final HashSet<Fragment> leafFragments = new HashSet<Fragment>();
    	if (logger.isDebugEnabled())
			logger.debug("RETURN getLeafFragments()"); 
    	return this.getLeafFragments(this.getRootFragment(), leafFragments);
    }
    
    /**
     * Given a physical query plan with exchange operators inserted, 
     * constructs the fragment tree. 
     */
    //TODO: Make this a private method??
    public final void buildFragmentTree() {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER buildFragmentTree()"); 
		boolean newFrag = true;
		Fragment currentFrag = this.rootFragment;
	
		final Iterator<SensornetOperator> opIter = this
			.operatorIterator(TraversalOrder.PRE_ORDER);
		while (opIter.hasNext()) {
		    final SensornetOperator op = opIter.next();
	
		    if (op instanceof SensornetExchangeOperator) {
				//new fragment indicated by exhange
				//link the current fragment to the exchange operator
				currentFrag = op.getParent().getContainingFragment();
				((SensornetExchangeOperator) op).setDestinationFragment(currentFrag);
				currentFrag.addChildExchange((SensornetExchangeOperator) op);
				// create a new fragment
				currentFrag = new Fragment();
				this.fragments.add(currentFrag);
				newFrag = true;
				// link the new fragment to the exchange operator
				((SensornetExchangeOperator) op).setSourceFragment(currentFrag);
				currentFrag.setParentExchange((SensornetExchangeOperator) op);
		    } else if (newFrag) {
				//first operator in each fragment 
		    	//(i.e., the fragment's root operator)
				currentFrag.setRootOperator(op);
				newFrag = false;
				currentFrag.addOperator(op);
				op.setContainingFragment(currentFrag);
		    } else {
				//all other operators within a fragment
				currentFrag = ((SensornetOperator) op.getParent())
					.getContainingFragment();
				currentFrag.addOperator(op);
				op.setContainingFragment(currentFrag);
		    }
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN buildFragmentTree()"); 
    }
    
    /**
     * Helper method to recursively generate the operator iterator.
     * @param frag the fragment being visited
     * @param fragList the operator list being created
     * @param traversalOrder the traversal order desired 
     */    
    private void doFragmentIterator(final Fragment frag,
    final ArrayList<Fragment> fragList, final TraversalOrder traversalOrder) {
    	if (logger.isTraceEnabled())
			logger.trace("ENTER doFragmentIterator()"); 
    	if (traversalOrder == TraversalOrder.PRE_ORDER) {
    	    fragList.add(frag);
    	}

    	for (int n = 0; n < frag.getChildFragments().size(); n++) {

    	    this.doFragmentIterator(frag.getChildFragments().get(n), 
    	    		fragList, traversalOrder);
    	}

    	if (traversalOrder == TraversalOrder.POST_ORDER) {
    	    fragList.add(frag);
    	}
    	if (logger.isTraceEnabled())
			logger.trace("RETURN doFragmentIterator()"); 
    }

    /**
     * Iterator to traverse the operator tree.
     * The structure of the operator tree may not be modified during iteration
     * @param traversalOrder the order to traverse the operator tree
     * @return an iterator for the operator tree
     */
    public final Iterator<Fragment> fragmentIterator(final 
    TraversalOrder traversalOrder) {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER fragmentIterator()"); 
    	final ArrayList<Fragment> fragList = new ArrayList<Fragment>();
    	this.doFragmentIterator(this.getRootFragment(), fragList,
    		traversalOrder);
    	if (logger.isDebugEnabled())
			logger.debug("RETURN fragmentIterator()");
    	return fragList.iterator();
    }

	public void insertOperator(SensornetOperator child, SensornetOperator parent,
			SensornetExchangeOperator newOperator) {
		this.getPAF().insertOperator(child, parent, newOperator);
	}

    /**
     * Places the given fragment at the given sensor network node; 
     * note that the same fragment be be placed at several sites. 
     * @param frag The fragment to be placed
     * @param site The site at which the fragment is to be placed.
     */
    public final void placeFragment(final Fragment frag, final Site site) {
		//update the sensor network node in the routingTree		
		site.addFragment(frag);
		//update the fragment
		frag.addSite(site);
    }
    
    /**
     * Places the given fragment at the given sensor network nodes.
     * @param frag The fragment to be placed
     * @param siteArray The site at which the fragment is to be placed
     */
    public final void placeFragment(
    		final Fragment frag, final ArrayList<Site> siteArray) {
		for (int i = 0; i < siteArray.size(); i++) {
		    this.placeFragment(frag, siteArray.get(i));
		}
    }
    
    /**
     * Links the copy of the source fragment allocated to source site to a 
     * copy of destination fragment executing on the destination site. 
     * @param sourceFrag the source fragment
     * @param sourceSite the source site
     * @param destFrag the destination fragment
     * @param destSite the destination site
     * @param path the path to be used to link the source fragment instance 
     * to the destination fragment instance.
     */
    public final void linkFragments(final Fragment sourceFrag, 
    		final Site sourceSite, final Fragment destFrag, 
    		final Site destSite, final Path path) {

		logger.trace("Linking F" + sourceFrag.getID() + " on site "
			+ sourceSite.getID() + " to F" + destFrag.getID() + " on site "
			+ destSite.getID());
		
		//check source and dest nodes are are first and last nodes in path, 
		//otherwise insert
		if (path.getFirstSite() != sourceSite) {
		    path.prepend(sourceSite);
		}
		if (path.getLastSite() != destSite) {
		    path.append(destSite);
		}
	
		//Update the routing table in the exchange operator
		final RoutingTableEntry entry = new RoutingTableEntry(destSite,
			destFrag, path);
		sourceFrag.getParentExchangeOperator().addRoutingEntry(sourceSite,
			entry);
	
		//Place the exchange components (producer, relay and consumer) on the 
		//respective sensor network nodes.  Check that producer and consumer 
		//are not on the same node; if they are, don't bother
		boolean isRemote = false;
		if (sourceSite != destSite) {
		    isRemote = true;
		}
	
		ExchangePart prev = null;
		Iterator<Site> pathIter = path.iterator();
		while (pathIter.hasNext()) {
			Site currentSite = pathIter.next();
			//insert the producer at the start of the path
			if (currentSite == sourceSite) {
				prev = new ExchangePart(sourceFrag, sourceSite, destFrag,
						destSite, currentSite,
						ExchangePartType.PRODUCER, isRemote, prev);
			}
			if (currentSite == destSite) {
				// insert the consumer at the end of the path
		    	prev = new ExchangePart(sourceFrag, sourceSite, destFrag,
				destSite, currentSite,
				ExchangePartType.CONSUMER, isRemote, prev);
		    }
			if (currentSite != sourceSite && currentSite != destSite) {
		    	//insert relays in intermediate nodes (only if applicable)
		    	prev = new ExchangePart(sourceFrag, sourceSite, destFrag,
				destSite, currentSite,
				ExchangePartType.RELAY, isRemote, prev);
		    }
		}
    }
    
    /**
     * Given a parent operator, the site of the parent operator, and an index 
     * of an operator child, returns the sites which that child is placed. 
     * @param op The parent operator. If op is an exchange it is assumed to be the producer.
     * @param site The site of the parent operator. 
     * 	Note for exchange operators this will be a consumer site
     * @param index The index of the child 
     * @return The sites on which the child of this operator is found.
     */
    public final Iterator<Site> getInputOperatorInstanceSites(
	    final SensornetOperator op, final Site site, final int index) {

		ArrayList<Site> results = new ArrayList<Site>();
		final SensornetOperator childOp = (SensornetOperator) op.getInput(index);
		if (childOp instanceof SensornetExchangeOperator) {
		    results = ((SensornetExchangeOperator) childOp).getSourceSites(site);
		} else {
		    results.add(site);
		}
	
		return results.iterator();
    }

	public SensornetOperator getRootOperator() {
		return (SensornetOperator) this.getOperatorTree().getRoot();
	}    

}


