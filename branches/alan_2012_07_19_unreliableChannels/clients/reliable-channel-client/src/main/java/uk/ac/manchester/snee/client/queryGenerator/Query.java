package uk.ac.manchester.snee.client.queryGenerator;


// Query: Representing a SQL Query, which can be generated from randomly
// selected tables in a given database


import java.util.*;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class Query {

    int id;
    boolean defined = false;

    String select; // The text of the SELECT clause
    Vector<Table> from; // The Table objects of the tables in the from clause
    String where; // The text of the WHERE clause

    public Query (int theId) 
    {
        id = theId;
        defined = false;
    }

    public int getId()
    {
        return id;
    }

    // Given a database schema consisting of a collection of tables,
    // a target number of joins, a probability that a local predicate
    // should be added (iteratively) for each join, and the probability
    // that a predicate should be an equality, populate the select,
    // from and where attributes.
    //
    public void buildQuery(Vector<?> tables, int numJoins, double predProb, 
                           double nowWindowProb, int acqRate, 
                           Double allAtributesProb, TPCH.queryType queryType) 
    {
        if (tables.size() < numJoins) {
            System.out.println("Not enough tables to perform " + numJoins + " joins on");
            return;
        }

        select = "SELECT ";
        from = new Vector<Table>();
        where = "WHERE ";

        boolean[] used = new boolean[tables.size()];
        Arrays.fill(used, (boolean) false);

        boolean firstFilter = true;
        
        for (int i = 1; i <= numJoins; i++) {

            // Identify the next table to add, avoiding duplicates

            boolean found = false;
            Table candidateTable = null;
            String candidateWhere = null;
            int tablePos = 0;

            while (!found) {

                // First find a random table that hasn't been used
                Random rnd = new Random();
                do {
                    tablePos = rnd.nextInt(tables.size());
                } while (used[tablePos]);

                candidateTable = (Table) tables.get(tablePos);
                // System.out.println("Candate Table: " + candidateTable.getName());

                if (i==1) 
                    // No existing table to join to
                    found = true;
                else {
                    // Check to see if it is related to a table
                    // that has been used

                    // First: does the candidateTable contain an attribute that
                    // is a foreign key for any table already in the from clause?
                    String matchingAttribute = null;
                    Table tableInUse = null;
                    Iterator<Table> it = from.iterator();
                    do {
                        tableInUse = (Table) it.next();
                        matchingAttribute = candidateTable.getForeignKey(tableInUse.getName());
                    } while (it.hasNext() && matchingAttribute == null);

                    found = (matchingAttribute != null);

                    if (found) {
                        candidateWhere = 
                            candidateTable.getName() + "." + matchingAttribute + 
                            " = " + 
                            tableInUse.getName() + "." + tableInUse.getKey();
                    } else {
    
                        // Second: does a table in the from clause contain an 
                        // attribute that is a foreign key for the candidateTable?
                        it = from.iterator();
                        do {
                            tableInUse = (Table) it.next();
                            matchingAttribute = tableInUse.getForeignKey(candidateTable.getName());
                        } while (it.hasNext() && matchingAttribute == null);

                        found = (matchingAttribute != null);

                        if (found) {
                            candidateWhere = 
                                candidateTable.getName() + "." + candidateTable.getKey() + 
                                " = " + 
                                tableInUse.getName() + "." + matchingAttribute;
                        }
                    }
                }
            }

            used[tablePos] = true;

            // System.out.println("Adding: " + candidateTable.getName());
            Random rnd = new Random();
            if(queryType.toString().equals(TPCH.queryType.SELECT.toString()) ||
               queryType.toString().equals(TPCH.queryType.JOIN.toString()))
            {
              if (i > 1) 
                select = select + ", ";
              
              if(rnd.nextInt(10) > (new Double(allAtributesProb*10).intValue()))
              {
                int numberofAttribputes = rnd.nextInt(candidateTable.getClonedAttributes().size()-1);
                if(numberofAttribputes == 0)
                  numberofAttribputes = 1;
                Vector<Attribute> possibleAttributes = candidateTable.getClonedAttributes();
                int done = 0;
                boolean first = true;
                while(done < numberofAttribputes)
                {
                  if(first)
                  {
                    int rnadomIndex = rnd.nextInt(possibleAttributes.size());
                    select = select + candidateTable.getName() + "." + 
                    possibleAttributes.get(rnadomIndex).name;
                    first = false;
                    possibleAttributes.remove(rnadomIndex);
                  }
                  else
                  {
                    int rnadomIndex = rnd.nextInt(possibleAttributes.size());
                    select = select + ", " + candidateTable.getName() + "." + 
                    possibleAttributes.get(rnadomIndex).name;
                    possibleAttributes.remove(rnadomIndex);
                  }
                  done++;
                }
              }
              else
                select = select + candidateTable.getName() + "." + "*";
            }
            else if(queryType.toString().equals(TPCH.queryType.AGG.toString()))
            {
              Vector<String> aggregationOperators = 
                new Vector<String>(Arrays.asList("AVG", "COUNT", "SUM", "MIN", "MAX"));
              int rnadomAggregationOperatorIndex = rnd.nextInt(aggregationOperators.size());
              String aggregationOperator = aggregationOperators.get(rnadomAggregationOperatorIndex);
              select = select + aggregationOperator + "(";
              int attributeIndex = rnd.nextInt(candidateTable.getAttributes().size());
              select = select.concat(candidateTable.getName()+ "." + 
                  candidateTable.getAttributes().get(attributeIndex).name + ") ");
            }
            
            
            //Add window definition
            int randomValue = rnd.nextInt(10);
            Window win = null;
            if(randomValue > (new Double(nowWindowProb*10).intValue()))
            {
              //range and slide must be multipliers of the acq rate
              int randomAcqMultiplier = rnd.nextInt(10);
              int range = acqRate * randomAcqMultiplier;
              randomAcqMultiplier = rnd.nextInt(10);
              int slide =  acqRate * randomAcqMultiplier;
              win = new Window(range, slide, Window.timeValue.SECONDS);
            }
            else
              win = new Window();
            candidateTable.setWindow(win);
            
            from.add(candidateTable);
            
            
            if (i == 2) 
            {
              where = where + candidateWhere;
              firstFilter = false;
            }
            if (i > 2)
            {
              where = where + " AND " + candidateWhere;
              firstFilter = false;
            }
        }

        String filter = this.buildFilter(predProb, firstFilter);
        if (filter.length() > 0) where = where +  this.buildFilter(predProb, firstFilter);

        defined = true;
    }

    // Given a probability that a further predicate should be added to
    // each table, add inequality predicates to the attributes of the
    // table, where they have type VARCHAR, INTEGER or DOUBLE
    //
    private String buildFilter(double predProb, boolean firstFilter) 
    {
        String filter = "";
        
        Iterator<Table> it = from.iterator();
        while (it.hasNext())
        {
            Table table = (Table) it.next();
            Vector<?> attributes = table.getAttributes();

            // Add predicates to randomly selected attributes, taking into 
            // account predProb; this assumes that every table has attributes
            // that are of type VARCHAR, INTEGER or DECIMAL, and works better
            // where there are plenty such attributes
            int numPreds = 0;
            Random rnd = new Random();
            while (rnd.nextDouble() < predProb && numPreds < attributes.size())
            {
                numPreds ++;
                // Randomly select an attribute to which to add a predicate
                Attribute attribute = (Attribute) attributes.get(rnd.nextInt(attributes.size()));
                String type = attribute.getType();

                String comparisonLiteral = null; 
                // Generate random values to compare with
                // Random value generation a bit hit or miss
                if (type.equalsIgnoreCase("INTEGER")) {
                    comparisonLiteral = Integer.toString(rnd.nextInt(1000));
                } else if (type.equalsIgnoreCase("DECIMAL")) {
                    comparisonLiteral = Double.toString(rnd.nextDouble()*1000);
                } else if (type.equalsIgnoreCase("VARCHAR")) {
                    String s = "aBcDeFgHiJkLmNoPqRsTuVwXyZ";
                    int pos = rnd.nextInt(s.length() - 1);
                    comparisonLiteral = "'" + s.substring(pos,pos+1) + "'";
                }

                if (comparisonLiteral != null) {
                    String comparisonOperator;
                    // Select a comparison operator at random
                    if (rnd.nextDouble() > 0.5) {
                        comparisonOperator = ">";
                    } else {
                        comparisonOperator = "<";
                    }
    
                    if (filter.length() > 0 || !firstFilter)
                      filter = filter + " AND ";
    
                    filter = filter + table.getName() + "." + attribute.getName() + 
                        comparisonOperator + comparisonLiteral;
                }
            }
        } 
        return filter;
    }
        

    public String toString()
    {
        if (defined) 
        {
            String theSQL = select + " FROM ";
            for (int i = 1; i <= from.size(); i++) 
            {
                if (i > 1) theSQL = theSQL + ", ";
                Table table = (Table) from.get(i - 1);
                theSQL = theSQL + table.getName() + table.getWindow().toString();
            }
            if(!where.equals("WHERE ")) 
              theSQL = theSQL + " " + where;

            return theSQL;

        } else {

            return "Undefined";

        }
    }
}
