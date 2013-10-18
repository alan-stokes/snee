package uk.ac.manchester.snee.client.queryGenerator;


// Attribute: Representing an Attribute within a Table
//

public class Attribute {

    String name;
    String type;
    boolean key; // Is this the key?
    String foreignKeyTable; // The name of any table for which this is FK

    // persistence-capable class types

    /** Creates new MyClass */
    public Attribute (String theName, String theType, boolean theKey) {
        name = theName;
        type = theType;
        key = theKey;
        foreignKeyTable = null;
    }

    public Attribute (String theName, String theType, boolean theKey, 
              String theForeignKeyTable) {
        name = theName;
        type = theType;
        key = theKey;
        foreignKeyTable = theForeignKeyTable;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public boolean getKey()
    {
        return key;
    }

    public String getForeignKeyTable()
    {
        return foreignKeyTable;
    }

    public String toString()
    {
        return "Attribute: " + name + "Of Type" + type + " Key = " + key;
    }
}
