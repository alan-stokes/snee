package uk.ac.manchester.cs.snee.compiler.queryplan;

public abstract class QueryExecutionPlan {

	DLAF dlaf;
	
	protected QueryExecutionPlan(DLAF dlaf) {
		this.dlaf = dlaf;
	}
	
	public DLAF getDLAF(){
		return this.dlaf;
	}
	
	public LAF getLAF() {
		return this.dlaf.getLAF();
	}

	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}
}
