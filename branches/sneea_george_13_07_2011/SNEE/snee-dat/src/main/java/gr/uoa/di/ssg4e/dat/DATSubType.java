package gr.uoa.di.ssg4e.dat;

/**
 * The different types for the Data Analysis Techniques
 * */
public enum DATSubType {

	/** Sub types of the CLASSIFICATION Family */
	KNN ( DATType.CLASSIFIER ), /** K-Nearest Neighbor */
	DecisionTree ( DATType.CLASSIFIER ), /** Decision Trees classification. Default for C4.5 */
	HoeffdingTree ( DATType.CLASSIFIER ), /** Decision Trees classification with Hoeffding Trees */
	LinearRegressionFunction (DATType.CLASSIFIER), /** Linear Regression, currently being implemented */
	
	/** Sub types of the CLUSTERING Family */
	KMEANS ( DATType.CLUSTER ), /** K-Nearest Neighbor */
	KMEDOIDS ( DATType.CLUSTER ), /** KMedoids clustering technique */
	DBSCAN ( DATType.CLUSTER ), /** DBScan technique */
	NHC ( DATType.CLUSTER ), /** Naive Hierarchical Clustering */
	
	/** Sub types of the OUTLIER DETECTION Family */
	D3 ( DATType.OUTLIER_DETECTION ), /** The D3 algo that we are currently implementing */
	
	/** Sub types of the PROBABILITY FUNCTIONS Family */
	KDE ( DATType.PROBABILITY_FUNCTION ), /** Kernel Density Estimators for probability functions */
	HISTOGRAMS ( DATType.PROBABILITY_FUNCTION ), /** Histograms */

	/** Sub types of the SAMPLING Family */
	RANDOM ( DATType.SAMPLING ), /** Random Sampling */
	RESERVOIR ( DATType.SAMPLING ), /** Reservoir Sampling */
	CHAIN ( DATType.SAMPLING ), /** Chain sampling */

	;
	
	/** True if this source returns a stream. */
	private final DATType type;

	/** 
	 * Constructor for a new DAT subtype. Need to specify a name
	 */
	private DATSubType(final DATType type) {
		this.type = type;
	}	
	
	public DATType getType() {
		return type;
	}

	/** 
	 * Returns the sub type that is equal to the given tag name. If the
	 * tag is not known, or if the associated typeTag is not correct,
	 * then null is returned
	 *  */
	public static DATSubType getTaggedSubType(String subTypeTag, String typeTag){

		if ( subTypeTag == null || typeTag == null )
			return null;

		DATSubType foundSubtype = null;

		if ( subTypeTag.equalsIgnoreCase(DATSubType.CHAIN.name()) ){
			foundSubtype = DATSubType.CHAIN;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.D3.name()) ){
			foundSubtype = D3;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.DBSCAN.name()) ){
			foundSubtype = DBSCAN;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.DecisionTree.name()) ){
			foundSubtype = DecisionTree;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.HISTOGRAMS.name()) ){
			foundSubtype = HISTOGRAMS;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.HoeffdingTree.name()) ){
			foundSubtype = HoeffdingTree;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.KDE.name()) ){
			foundSubtype = KDE;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.KMEANS.name()) ){
			foundSubtype = KMEANS;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.KMEDOIDS.name()) ){
			foundSubtype = KMEDOIDS;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.KNN.name()) ){
			foundSubtype = KNN;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.LinearRegressionFunction.name()) ){
			foundSubtype = LinearRegressionFunction;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.NHC.name()) ){
			foundSubtype = NHC;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.RANDOM.name()) ){
			foundSubtype = RANDOM;
		}else if ( subTypeTag.equalsIgnoreCase(DATSubType.RESERVOIR.name()) ){
			foundSubtype = RESERVOIR;
		}

		/* The given tag did not match any of the given subtypes */
		if ( foundSubtype == null )
			return null;

		/* Check if the type tag equals that of the found subytpe. If not
		 * null is returned */
		if ( !typeTag.equalsIgnoreCase(foundSubtype.getType().name()) )
			return null;

		/* The subtype was found and the type is correct. Return it */
		return foundSubtype;
	}
}
