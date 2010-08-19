package uk.ac.manchester.cs.snee.compiler.queryplan;

public abstract class SNEEAlgebraicForm {

	/**
	 * The identifier of this *AF.
	 */
	private String name;

	private String queryName;
	
	public SNEEAlgebraicForm(String queryName) {
		this.queryName = queryName;
		this.name = generateName(queryName);
	}
	
	protected abstract String generateName(String queryName);
	
	public void setName(String newLafName) {
		this.name = newLafName;
	}
	
	public String getQueryName() {
		return this.queryName;
	}
	
	public String getName() {
		return this.name;
	}
	
	public abstract String getProvenanceString();

}
