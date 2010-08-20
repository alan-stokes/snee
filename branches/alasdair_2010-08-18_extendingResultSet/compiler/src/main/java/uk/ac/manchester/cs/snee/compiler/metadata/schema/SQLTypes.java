package uk.ac.manchester.cs.snee.compiler.metadata.schema;

import java.sql.Types;

public enum SQLTypes {

	BOOLEAN (Types.BOOLEAN, "boolean"),
	FLOAT (Types.FLOAT, "float"),
	INTEGER (Types.INTEGER, "integer"),
	TIMESTAMP (Types.TIMESTAMP, "timestamp"),
	VARCHAR (Types.VARCHAR, "string");
	
	private String display;
	private int type;

	SQLTypes(int type, String display) {
		this.type = type;
		this.display = display;
	}
	
	public int getSQLType() {
		return type;
	}
	
	public String toString() {
		return display;
	}
	
}
