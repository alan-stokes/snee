package uk.ac.manchester.cs.snee.operators.sensornet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.RoutingTableEntry;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.logical.ProjectOperator;

public class SensornetExchangeOperator extends SensornetOperatorImpl {
	
	Logger logger = Logger.getLogger(SensornetExchangeOperator.class.getName());

	/**
	 * The destination fragment(s) of the exchange operator.
	 * An exchange operator may have more than one destination fragment in a
	 * multiple query execution context, i.e., the data flows are forked.
	 * Note that if the source fragment is recursive, it may send its output to
	 * other instances of the same fragment.
	 */
	private Fragment destFragment = null;
	
	/**
	 * The source fragment of the exchange operator.  Note that although a
	 * fragment may have more than one child fragment (e.g., in the case of a 
	 * join), this is implemented using a different exchange operator. 
	 */
	private Fragment sourceFragment;

	/**
	 * The paths used to link instances of the source fragment to instances 
	 * of the parent fragment.  Note that if the source fragment is recursive,
	 * it may be linked to a different instance of the same fragment.	 
	 * TODO: no checks are currently made to avoid circular paths!
	 */
	private HashMap<Site, RoutingTableEntry> sourceToDestPaths 
		= new HashMap<Site, RoutingTableEntry>();
	
	public SensornetExchangeOperator() 
	throws SNEEException, SchemaMetadataException {
		super();
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetExchangeOperator() ");
		}
		this.setOperatorName("EXCHANGE");
		this.setNesCTemplateName("producer");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetExchangeOperator()");
		}
	}

    /**
     * Returns the destination fragments of this exchange operator.
     * @return the destination fragments
     */
    public final Fragment getDestFragment() {
    	return this.destFragment;
    }	
	
    /**
     * Returns the source fragments of this exchange operator.
     * @return the source fragments
     */
    public final Fragment getSourceFragment() {
    	return this.sourceFragment;
    }
     
    /**
     * Add a destination fragment for this exchange operator.
     * @param frag the destination fragment
     */
    public final void setDestinationFragment(final Fragment frag) {
    	assert (frag != null);
    	this.destFragment = frag;
    }

    /**
     * Sets the source fragment for this exchange operator.
     * @param frag the source fragment
     */
    public final void setSourceFragment(final Fragment frag) {
    	this.sourceFragment = frag;
    }
    
    /**
     * Output a String representation of the operator.
     * @return a String representation of the operator.
     */
    @Override
	public final String toString() {
    	return this.getOperatorName();
    }

	public final void addRoutingEntry(final Site sourceSite, final RoutingTableEntry entry) { 
		this.sourceToDestPaths.put(sourceSite, entry);
	}

	public ArrayList<Site> getSourceSites(final Site consumerSite) {
		
		final ArrayList<Site> results = new ArrayList<Site>();
		
		Iterator<Site> allSourceNodes 
			= this.sourceToDestPaths.keySet().iterator();
		while (allSourceNodes.hasNext()) {
			final Site s = allSourceNodes.next();
			if ((this.sourceToDestPaths.get(s).getSite() == consumerSite)) {
				results.add(s);
			}
		}
		if (destFragment == null) {
			return results;
		}
		
		if (destFragment.isRecursive()) {
			final HashMap<Site, RoutingTableEntry> parentExchangeRoutingTable 
				= ((SensornetExchangeOperator)destFragment.getParentExchangeOperator()).sourceToDestPaths; 
			
			allSourceNodes = parentExchangeRoutingTable.keySet().iterator();
			while (allSourceNodes.hasNext()) {
				final Site s = allSourceNodes.next();
				if ((parentExchangeRoutingTable.get(s).getSite() 
						== consumerSite) && (parentExchangeRoutingTable.get(s)
						.getFragment() == destFragment)) {
					results.add(s);
				}
			}
		}
		
		return results;
	}
	
	public String getDOTRoutingString() {
    	final StringBuffer s = new StringBuffer();
    	
    	final Iterator<Site> routingFunctionIter = 
    		this.sourceToDestPaths.keySet().iterator();
    	while (routingFunctionIter.hasNext()) {
    		final Site sourceNode = routingFunctionIter.next();
    		
    		final RoutingTableEntry entry = this.sourceToDestPaths.get(sourceNode);
    		s.append("n" + sourceNode.getID() + " -> ");
    		s.append("n" + entry.getSiteID());
    		s.append("f" + entry.getFragID());
    		
    		s.append(" [");
    		boolean first = true;
    		final Iterator<Site> pathIter = entry.getPath().iterator();
    		while (pathIter.hasNext()) {
    			if (!first) {
					s.append(",");
				}
    			final Site n = pathIter.next();
    			s.append(n.getID());
    			first = false;
    		}
    		s.append("]");

    		s.append("\\n");

    	}
    	
    	return s.toString();
	}
    
}
