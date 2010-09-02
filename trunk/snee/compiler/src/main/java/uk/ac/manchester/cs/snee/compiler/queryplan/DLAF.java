package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;

/**
 * The logical algebraic form of the query plan operator tree partitioned
 * by source (i.e., sensor network, evaluator).  For now, query plans are
 * either executed wholly within the sensor network, or wholly within the
 * evaluator.
 *
 */
public class DLAF {

    /**
     * Logger for this class.
     */
    private static  Logger logger = 
    	Logger.getLogger(DLAF.class.getName());
	
    private String name;
    
	/**
	 * The logical-algebraic form of the query plan operator tree 
	 * from which DLAF is derived.
	 */	
	private LAF laf;

	/**
	 * The type of source this query operator tree is for.  In the future,
	 * it will be possible to allocate different portions of the LAF to different
	 * sourceTypes.
	 */
//	private SourceType[] sourceType;

	/**
	 * The sources that are used in this query operator tree.
	 */
	private List<SourceMetadata> sources = 
		new ArrayList<SourceMetadata>();
	
    /**
     * Counter to assign unique id to different candidates.
     */
    protected static int candidateCount = 0;
    
	/**
	 * Implicit constructor used by subclass.
	 */
	protected DLAF() { }
	
	/**
	 * Constructor for DLAF
	 * @param laf The logical-algebraic form of the query plan operator tree 
	 * from which DLAF is derived.
	 * @param queryName The name of the query
	 */
	public DLAF(final LAF laf, final String queryName) {
		this.laf = laf;
		this.name = generateName(queryName);
	}
	
//    /**
//     * Constructor used by clone.
//     * @param dlaf The DLAF to be cloned
//     * @param inName The name to be assigned to the data structure
//     */
//	public DLAF(final DLAF dlaf, final String inName) {
//		super(dlaf, inName);
//		
//		this.laf = dlaf.laf; //This is ok because the laf is immutable now
//	}
	
    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }
	
	/**
	 * Generates a systematic name for this query plan structure, of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
    private static String generateName(final String queryName) {
    	candidateCount++;
    	return queryName + "-DLAF-" + candidateCount;
    }
    
	public String getProvenanceString() {
		return this.laf.getProvenanceString() + "->" + this.name;
	}
//
//	public void setSourceType(SourceType sourceType) {
//		this.sourceType = sourceType;
//	}

	public void setSources(List<SourceMetadata> sources) {
		this.sources.addAll(sources);
	}
	
//	public SourceType getSourceType() {
//		return this.sourceType;
//	}
	
	public List<SourceMetadata> getSources() {
		return sources;
	}

	public LAF getLAF() {
		return this.laf;
	}

	public String getName() {
		return this.name;
	}
	
}
