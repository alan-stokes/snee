package uk.ac.manchester.snee.client.queryGenerator;


// Table: Representing a Relation Over Which Queries May Act
//

import java.util.*;

public class Table {

    String name;
    Vector<Attribute> attributes;
    Window window;

    // persistence-capable class types

    /** Creates new MyClass */
    public Table (String theName, Vector<Attribute> theAttributes) {
        name = theName;
        attributes = theAttributes;
    }

    public String getName()
    {
        return name;
    }

    public Vector<Attribute> getAttributes()
    {
        return attributes;
    }

    // Return the name of the first key attribute.
    // Assumes there is a key attribute
    public String getKey()
    {
        Attribute att = null;
        Iterator<?> i = attributes.iterator();
        do {
            att = (Attribute) i.next();
        } while (i.hasNext() && !att.getKey());

        return att.getName();
    }

    // Given the name of a table, return the name of any 
    // attribute that is a foreign key for that table
    public String getForeignKey(String tableName)
    {
        Attribute att = null;
        String res = null;
        boolean found = false;
        Iterator<Attribute> i = attributes.iterator();
        do {
            att = (Attribute) i.next();
            String fk = att.getForeignKeyTable();
            if (fk != null && fk.equalsIgnoreCase(tableName)) {
                found = true;
                res = att.getName();
            }
        } while (i.hasNext() && !found);

        return res;
    }

    public String toString()
    {
      return "Table: " + name + window.toString();
    }
    
    public void setWindow(Window win)
    {
      window = win;
    }
    
    public Window getWindow()
    {
      return window;
    }

    public Vector<Attribute> getClonedAttributes()
    {
      Iterator<Attribute> attributeIteratoer = attributes.iterator();
      Vector<Attribute> clone = new Vector<Attribute>();
      while(attributeIteratoer.hasNext())
      {
        clone.add(attributeIteratoer.next());
      }
      return clone;
    }
}
