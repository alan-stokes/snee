package uk.ac.manchester.cs.snee.compiler.sn.where;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class WhereScheduler {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(WhereScheduler.class.getName());

	/**
	 * Constructor for Sensor Network Where-Scheduling Decision Maker.
	 */
	public WhereScheduler() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER WhereScheduler()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN WhereScheduler()");
	}

	/**
	 * Carry out the sensor network routing.
	 * @param paf
	 * @param rt 
	 * @param queryName
	 * @return
	 * @throws SchemaMetadataException 
	 * @throws SNEEException 
	 * @throws OptimizationException 
	 */
	public DAF doWhereScheduling(PAF paf, RT rt, String queryName) 
	throws SNEEException, SchemaMetadataException, OptimizationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doWhereScheduling() with " + paf.getID());
		DAF faf = partitionPAF(paf, queryName);
		placeFragments(queryName, faf, rt);
		if (logger.isDebugEnabled())
			logger.debug("RETURN doWhereScheduling()");
		return faf; //DAF
	}
	
	
	private DAF partitionPAF(final PAF paf, final String queryName) 
	throws SNEEException, SchemaMetadataException {
		DAF faf = new DAF(paf, queryName);
		final Iterator<SensornetOperator> opIter = faf
			.operatorIterator(TraversalOrder.POST_ORDER);
		
   		//CB never used OperatorImplementation.setOpCountResetVal();
 		while (opIter.hasNext()) {
	
			    final SensornetOperator op = (SensornetOperator) opIter.next();
		
			    if (!op.isLeaf()) {
				for (int childIndex = 0; childIndex < op.getInDegree(); childIndex++) {
				    final SensornetOperator c = (SensornetOperator) op.getInput(childIndex);
				    /* Communications may be desirable between an operator 
				     * and one of its children. 
				     * If it has a larger output size than that child,
				     * and below AggrEval and AggrPartial operators,
				     * and above AggrEval operators
				     * and below all attribute sensitive operator
				     */
				    if ((op.getCardinality(CardinalityType.PHYSICAL_MAX) > c
					    .getCardinality(CardinalityType.PHYSICAL_MAX))
					    || (op.isAttributeSensitive())
					    || (op instanceof SensornetAggrMergeOperator)
					    || (op instanceof SensornetDeliverOperator)
					    || (c instanceof SensornetAggrEvalOperator)) {
					final SensornetExchangeOperator exchOp = 
						new SensornetExchangeOperator();
					faf.insertOperator(c, op, exchOp);
				    }
				}
		    }
		}
	
		logger.trace("Constructing query plan fragment tree.");
		faf.buildFragmentTree();
		return faf;
	}
	
    public void placeFragments(final String queryName, 
			final DAF daf, 
			final RT rt) throws OptimizationException {
		
    	int sink = new Integer(rt.getRoot().getID());
    	
		Fragment prevFrag = null;
 		
		final Iterator<Fragment> fragIter = daf
			.fragmentIterator(TraversalOrder.POST_ORDER);
		while (fragIter.hasNext()) {
 	 	
		    final Fragment frag = fragIter.next();
		    if (frag.isLocationSensitive()) {
			int[] sites;
//			data source 
			//Add scanOperator here
			if (frag.containsOperatorType(SensornetAcquireOperator.class)) {
			    sites = frag.getSourceNodes(); 		
			} else { //deliver
			    sites = new int[] { sink };
			}
 		
			frag.addDesiredSites(sites, rt);
			placeLocationSensitiveFragment(daf, rt, frag);
 		
		    } else if (frag.isRecursive()) {
			placeRecursiveFragment(daf, rt, frag);
 		
		    } else if (frag.isAttributeSensitive() && (prevFrag.isRecursive())) {
			//i.e., used by fragments containing AggrEval
			placeAttributeSensitiveOverRecursiveFragment(daf, rt, frag);
 		
		    } else if (frag.isAttributeSensitive()) {
			placeAttributeSensitiveFragment(daf, rt, frag);
 		
			//TODO: property: floating fragment: place a copy of each fragment for each copy of its children
			//TODO: assumes pull down advantageous
		    } else if (frag.getChildFragments().size() == 1) {
			placeFloatingFragment(daf, rt, frag);
		    } else {
			throw new OptimizationException("Unable to place fragment " + frag.getID());
		    }
 		
		    prevFrag = frag;
		}
	}

    /**
     * Places a location sensitive fragment at the required sites.
     * @param plan
     * @param frag
     */
    private void placeLocationSensitiveFragment(final DAF daf,
    RT rt, final Fragment frag) throws OptimizationException {

		if (frag.getDesiredSites().size() < 1) {
		    throw new OptimizationException(
			    "Required sites not specified for location sensitive fragment "
				    + frag.getID());
		}

		logger.trace("Placing location sensitive fragment " + frag.getID()
		+ " on required sites " + frag.getDesiredSitesString());

		ArrayList<Site> desiredSites = rt.getSites(frag.getDesiredSites());
		daf.placeFragment(frag, desiredSites);

		for (int i = 0; i < frag.getNumChildFragments(); i++) {
		    final Fragment childFrag = frag.getChildFragments().get(i);
	
		    final Iterator<Site> siteIter = childFrag.getSites().iterator();
		    while (siteIter.hasNext()) {
			final Site childSite = siteIter.next();
	
			final Path path = rt
				.getPath(childSite.getID(), frag.getDesiredSites().get(0));
	
			//currently assumes that there is only one desired site for non-leaf nodes.
			//TODO: need to check
			daf.linkFragments(childFrag, childSite, frag, rt.getSite(frag.
					getDesiredSites().get(0)), path);
		    }
		}
    }

    /**
     * Places a recursive fragment (i.e., one which may send output tuples to another copy of itself, 
     * required for incremental aggregation) as close to the sources as possible.  Note: It is assumed
     * that a recursive fragment only has one child fragment.
     *  
     * @param plan
     * @param currentNode
     * @param frag
     */
    private void placeRecursiveFragment(final DAF daf,
	    RT rt, final Fragment frag) {
	final Fragment childFrag = frag.getChildFragments().get(0);

	final HashMap<Site, Fragment> nodePrevFrag = new HashMap<Site, Fragment>();
	final HashMap<Site, Site> nodePrevNode = new HashMap<Site, Site>();
	final HashMap<Site, Path> nodePrevPath = new HashMap<Site, Path>();

	logger.trace("Placing recursive fragment " + frag.getID());

	final Iterator<Site> siteIter = rt.siteIterator(TraversalOrder.POST_ORDER);
	while (siteIter.hasNext()) {
	    final Site currentNode = siteIter.next();

	    logger.trace("Visiting site " + currentNode.getID());

	    //POST-ORDER code goes here
	    if (currentNode.isLeaf()) {

		//first occurrence of previous fragment
		if (childFrag.getSites().contains(currentNode)) {
		    nodePrevFrag.put(currentNode, childFrag);
		    nodePrevNode.put(currentNode, currentNode);
		    nodePrevPath.put(currentNode, new Path());
		    //no occurrence of previous fragment
		} else {
		    nodePrevFrag.put(currentNode, null);
		    nodePrevNode.put(currentNode, null);
		    nodePrevPath.put(currentNode, null);
		}
	    } else {

		//Sensor network node in routing tree with only one child
		if (currentNode.getInDegree() == 1) {

		    //check if previous fragment has been placed on this node
		    if (childFrag.getSites().contains(currentNode)) {

			//check if a copy of the previous or current fragment has been placed upstream
			if (nodePrevFrag.get(currentNode.getInput(0)) != null) {
			    //place here a copy of the current fragment
			    daf.placeFragment(frag, currentNode);

			    //link upstream fragment to current fragment
			    daf.linkFragments(nodePrevFrag.get(currentNode
				    .getInput(0)), nodePrevNode.get(currentNode
				    .getInput(0)), frag, currentNode,
				    nodePrevPath.get(currentNode.getInput(0)));

			    //link previous fragment on current node to current fragment
			    daf.linkFragments(childFrag, currentNode, frag,
				    currentNode, new Path());

			    //update nodePrevFrag and nodePrevNode
			    nodePrevFrag.put(currentNode, frag);
			    nodePrevNode.put(currentNode, currentNode);
			    nodePrevPath
				    .put(currentNode, new Path());

			} else {
			    // first occurence of previous fragment in this branch
			    nodePrevFrag.put(currentNode, childFrag);
			    nodePrevNode.put(currentNode, currentNode);
			    nodePrevPath
				    .put(currentNode, new Path());
			}
		    } else {
			//if previous fragment has not been placed on this node, propagate childs values for
			//nodePrevFrag and nodePrevNode, and incrementally construct path
			nodePrevFrag.put(currentNode, nodePrevFrag
				.get(currentNode.getInput(0)));
			nodePrevNode.put(currentNode, nodePrevNode
				.get(currentNode.getInput(0)));

			final Path p = nodePrevPath.get(currentNode
				.getInput(0));
			//quick-fix
			if (p != null) {
			    p.append(currentNode);
			    nodePrevPath.put(currentNode, p);
			} else {
			    nodePrevPath.put(currentNode, null);
			}
		    }

		    //Sensor network node with multiple children 
		} else {
		    //count the number of child nodes which have copies of the previous or current 
		    //fragment executing downstream
		    int flaggedChildCount = 0;
		    int flaggedChildIndex = -1;
		    for (int i = 0; i < currentNode.getInDegree(); i++) {
			if (nodePrevFrag.get(currentNode.getInput(i)) != null) {
			    flaggedChildCount++;
			}
			flaggedChildIndex = i; //TODO: check whether this should be in curly braces
		    }
		    logger.trace("Number of flagged children: "
			    + flaggedChildCount);

		    //number of flagged children == 1 and prev frag executing on current node or
		    //number of flagged children > 1
		    if (((flaggedChildCount == 1) && childFrag.getSites()
			    .contains(currentNode))
			    || (flaggedChildCount > 1)
			    || (childFrag.getSites().contains(currentNode) && (childFrag
				    .getSites().size() == 1))) { //try this
			//place current fragment on current node
			daf.placeFragment(frag, currentNode);

			//link prev frag in each child node as necessary
			for (int i = 0; i < currentNode.getInDegree(); i++) {
			    if (nodePrevFrag.get(currentNode.getInput(i)) != null) {
				daf.linkFragments(nodePrevFrag.get(currentNode
					.getInput(i)), nodePrevNode
					.get(currentNode.getInput(i)), frag,
					currentNode, nodePrevPath
						.get(currentNode.getInput(i)));
			    }
			}

			//link prev frag in current node if necessary
			if (childFrag.getSites().contains(currentNode)) {
			    daf.linkFragments(childFrag, currentNode, frag,
				    currentNode, new Path());
			}

			//update nodePrevFrag and nodePrevNode
			nodePrevFrag.put(currentNode, frag);
			nodePrevNode.put(currentNode, currentNode);
			nodePrevPath.put(currentNode, new Path());

			//child fragment on child node - propagate values
		    } else if (flaggedChildCount == 1) {

			nodePrevFrag.put(currentNode, nodePrevFrag
				.get(currentNode.getInput(flaggedChildIndex)));
			nodePrevNode.put(currentNode, nodePrevNode
				.get(currentNode.getInput(flaggedChildIndex)));

			final Path p = nodePrevPath.get(currentNode
				.getInput(flaggedChildIndex));
			p.append(currentNode);
			nodePrevPath.put(currentNode, p);
		    } else {
			nodePrevFrag.put(currentNode, null);
			nodePrevNode.put(currentNode, null);
			nodePrevPath.put(currentNode, null);
		    }
		}
	    }
	}
    }

    /**
     * Places an attribute sensitive fragment (i.e., one which requires all the tuples from the previous
     * fragments) as far upstream as possible.
     * 
     * @param plan
     * @param currentNode
     * @param frag
     */
    private void placeAttributeSensitiveFragment(final DAF daf,
	    RT rt, final Fragment frag) {
	final HashMap<Site, HashSet<Site>> nodePrevNodes = 
		new HashMap<Site, HashSet<Site>>();
	final HashMap<Site, Path> nodePrevPath = new HashMap<Site, Path>();

	logger.trace("Placing attribute sensitive fragment " + frag.getID());

	final Iterator<Site> siteIter = rt.siteIterator(TraversalOrder.POST_ORDER);
	while (siteIter.hasNext()) {
	    final Site currentNode = siteIter.next();

	    logger.trace("Visiting site " + currentNode.getID());

	    //POST-ORDER code goes here
	    final HashSet<Site> prevNodes = new HashSet<Site>();
	    final Path prevPath = new Path();

	    //check if previous fragment has been placed on this node
	    if (frag.getChildFragSites().contains(currentNode)) {
	    	prevNodes.add(currentNode);
	    }

	    //look at how many copies of previous fragment have been enountered in each child of the current node
	    for (int i = 0; i < currentNode.getInDegree(); i++) {
	    	prevNodes.addAll(nodePrevNodes.get(currentNode.getInput(i)));
	    }

	    nodePrevNodes.put(currentNode, prevNodes);
	    nodePrevPath.put(currentNode, prevPath);

	    //not all copies of previous fragment encountered, do routing
	    if (prevNodes.size() < frag.getChildFragSites().size()) {

		final Iterator<Site> snIterator = nodePrevNodes
			.get(currentNode).iterator();
		while (snIterator.hasNext()) {
		    final Site n = snIterator.next();

		    if (n != currentNode) {
			nodePrevPath.get(n).append(currentNode);
		    }
		}

	    } else {
		//all copies of previous fragments encountered, place current fragment at current node
		daf.placeFragment(frag, currentNode);

		//link each copy of each child fragment to the current fragment
		final Iterator<Fragment> childFragIter = frag
			.getChildFragments().iterator();
		while (childFragIter.hasNext()) {
		    final Fragment childFrag = childFragIter.next();

		    final Iterator<Site> snIterator = childFrag.getSites()
			    .iterator();
		    while (snIterator.hasNext()) {
			final Site n = snIterator.next();
			final Path path = nodePrevPath.get(n);

			daf.linkFragments(childFrag, n, frag, currentNode,
				path);
		    }
		}

		break;
	    }
	}
    }

    private void placeAttributeSensitiveOverRecursiveFragment(
	    final DAF daf, RT rt, final Fragment frag) {

	logger.trace("Placing Attribute Sensitive Fragment over a recursive fragment "
			+ frag.getID());

	final Fragment prevFrag = frag.getChildFragments().get(0);
	
	int prevFragNumEncountered = 0;

	final Iterator<Site> siteIter = rt.siteIterator(TraversalOrder.POST_ORDER);
	while (siteIter.hasNext()) {
	    final Site currentNode = siteIter.next();

	    logger.trace("Visiting site " + currentNode.getID());

	    if (prevFrag.getSites().contains(currentNode)) {
	    	prevFragNumEncountered++;
	    	logger.trace("prevFragNumEncountered="+prevFragNumEncountered);
	    }

	    if (prevFragNumEncountered == prevFrag.getSites().size()) {

		daf.placeFragment(frag, currentNode);
		daf.linkFragments(prevFrag, currentNode, frag, currentNode,
			new Path());
		break;
	    }
	}
    }

    private void placeFloatingFragment(final DAF daf,
	    RT rt, final Fragment frag) {

		logger.trace("Placing floating fragment " + frag.getID());
	
		//TODO: consider possibility that floating fragment might have more than one child (e.g. union op)
		final Fragment prevFrag = frag.getChildFragments().get(0);
	
		final Iterator<Site> siteIter = rt.siteIterator(TraversalOrder.POST_ORDER);
		while (siteIter.hasNext()) {
		    final Site currentNode = siteIter.next();
	
		    logger.trace("Visiting site " + currentNode.getID());
	
		    if (prevFrag.getSites().contains(currentNode)) {
	
			daf.placeFragment(frag, currentNode);
			daf.linkFragments(prevFrag, currentNode, frag, currentNode, new Path());
		    }
		}
    }

	
}
