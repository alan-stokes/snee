package uk.ac.manchester.cs.snee.compiler.metadata.schema;

@Deprecated
public class Attribute {

	private String _name;
	private AttributeType _type;
	
	public Attribute(String name, AttributeType type) {
		_name = name;
		_type = type;
	}

	public String get_name() {
		return _name;
	}

	public AttributeType get_type() {
		return _type;
	}
	
	@Override
	public boolean equals(Object ob) {
		boolean result = false;
		if (ob instanceof Attribute) {
			Attribute attr = (Attribute) ob;
			if (attr.get_name().equalsIgnoreCase(_name) &&
					attr.get_type() == _type)
					result = true;
		}
		return result;
	}
}
