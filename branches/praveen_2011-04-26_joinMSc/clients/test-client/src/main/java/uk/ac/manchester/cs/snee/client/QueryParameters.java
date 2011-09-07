package uk.ac.manchester.cs.snee.client;

/**
 * 
 */

/**
 * @author Praveen
 * 
 */
public class QueryParameters {

	private String queryId;
	private String query;
	private double duration;
	private String queryParamsPath;

	/**
	 * @return the queryId
	 */
	public String getQueryId() {
		return queryId;
	}

	/**
	 * @param queryId
	 *            the queryId to set
	 */
	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	/**
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @param query
	 *            the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return the duration
	 */
	public double getDuration() {
		return duration;
	}

	/**
	 * @param duration
	 *            the duration to set
	 */
	public void setDuration(double duration) {
		this.duration = duration;
	}

	/**
	 * @return the queryParamsPath
	 */
	public String getQueryParamsPath() {
		return queryParamsPath;
	}

	/**
	 * @param queryParamsPath
	 *            the queryParamsPath to set
	 */
	public void setQueryParamsPath(String queryParamsPath) {
		this.queryParamsPath = queryParamsPath;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("QueryParameters [queryId=");
		builder.append(queryId);
		builder.append(", query=");
		builder.append(query);
		builder.append(", duration=");
		builder.append(duration);
		builder.append(", queryParamsPath=");
		builder.append(queryParamsPath);
		builder.append("]");
		return builder.toString();
	}

}
