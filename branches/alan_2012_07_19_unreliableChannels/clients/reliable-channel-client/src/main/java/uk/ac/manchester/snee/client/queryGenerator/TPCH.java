package uk.ac.manchester.snee.client.queryGenerator;


// TPCH: Generate Example Queries Based on TPC-H Schema

import java.util.*;

public class TPCH {
  
  public static enum queryType{SELECT, AGG, JOIN}

    public static void main(String[] args) {
        int QoSAcqusitionRate = 10;
        double probabilyOfAllAttributes = 0.5;
        Vector<Attribute> A = new Vector<Attribute>();

        A.add(new Attribute("x","INTEGER",true));
        A.add(new Attribute("y","INTEGER",false));
        A.add(new Attribute("z","INTEGER",false));
        A.add(new Attribute("a","INTEGER",false));

        Table a = new Table("A", A);
        
        Vector<Attribute> B = new Vector<Attribute>();

        B.add(new Attribute("x","INTEGER",true));
        B.add(new Attribute("y","INTEGER",true,"A"));
        B.add(new Attribute("z","INTEGER",false));
        B.add(new Attribute("a","INTEGER",false));

        Table b = new Table("B", B);

	// ----------------------------------------------

        Vector<Attribute> regionAtts = new Vector<Attribute>();

        regionAtts.add(new Attribute("R_REGIONKEY","INTEGER",true));
        regionAtts.add(new Attribute("R_NAME","VARCHAR",false));
        regionAtts.add(new Attribute("R_COMMENT","VARCHAR",false));

        Table region = new Table("REGION", regionAtts);



        Vector<Attribute> partAtts = new Vector<Attribute>();

        partAtts.add(new Attribute("P_PARTKEY","INTEGER",true));
        partAtts.add(new Attribute("P_NAME","VARCHAR",false));
        partAtts.add(new Attribute("P_MFRG","VARCHAR",false));
        partAtts.add(new Attribute("P_BRAND","VARCHAR",false));
        partAtts.add(new Attribute("P_TYPE","VARCHAR",false));
        partAtts.add(new Attribute("P_SIZE","INTEGER",false));
        partAtts.add(new Attribute("P_CONTAINER","VARCHAR",false));
        partAtts.add(new Attribute("P_RETAILPRICE","DECIMAL",false));
        partAtts.add(new Attribute("P_COMMENT","VARCHAR",false));

        Table part = new Table("PART", partAtts);



        Vector<Attribute> supplierAtts = new Vector<Attribute>();

        supplierAtts.add(new Attribute("S_SUPPKEY","INTEGER",true));
        supplierAtts.add(new Attribute("S_NAME","VARCHAR",false));
        supplierAtts.add(new Attribute("S_ADDRESS","VARCHAR",false));
        supplierAtts.add(new Attribute("S_NATIONKEY","INTEGER",true,"NATION"));
        supplierAtts.add(new Attribute("S_PHONE","VARCHAR",false));
        supplierAtts.add(new Attribute("S_ACCTBAL","DECIMAL",false));
        supplierAtts.add(new Attribute("S_COMMENT","VARCHAR",false));

        Table supplier = new Table("SUPPLIER", supplierAtts);



        Vector<Attribute> psAtts = new Vector<Attribute>();

        psAtts.add(new Attribute("PS_PARTKEY","INTEGER",true,"PART"));
        psAtts.add(new Attribute("PS_SUPPKEY","INTEGER",true,"SUPPLIER"));
        psAtts.add(new Attribute("PS_AVAILQTY","INTEGER",false));
        psAtts.add(new Attribute("PS_SUPPLYCOST","DECIMAL",false));
        psAtts.add(new Attribute("PS_COMMENT","VARCHAR",false));

        Table ps = new Table("PARTSUPP", psAtts);



        Vector<Attribute> custAtts = new Vector<Attribute>();

        custAtts.add(new Attribute("C_CUSTKEY","INTEGER",true));
        custAtts.add(new Attribute("C_NAME","CARCHAR",false));
        custAtts.add(new Attribute("C_ADDRESS","VARCHAR",false));
        custAtts.add(new Attribute("C_NATIONKEY","INTEGER",false,"NATION"));
        custAtts.add(new Attribute("C_PHONE","VARCHAR",false));
        custAtts.add(new Attribute("C_ACCBAL","DECIMAL",false));
        custAtts.add(new Attribute("C_MKTSEGMENT","VARCHAR",false));
        custAtts.add(new Attribute("C_COMMENT","VARCHAR",false));

        Table cust = new Table("CUSTOMER", custAtts);



        Vector<Attribute> orderAtts = new Vector<Attribute>();

        orderAtts.add(new Attribute("O_ORDERKEY","INTEGER",true));
        orderAtts.add(new Attribute("O_CUSTKEY","INTEGER",true,"CUSTOMER"));
        orderAtts.add(new Attribute("O_ORDERSTATUS","VARCHAR",false));
        orderAtts.add(new Attribute("O_TOTALPRICE","DECIMAL",false));
        orderAtts.add(new Attribute("O_ORDERDATE","DATE",false));
        orderAtts.add(new Attribute("O_ORDERPRIORITY","VARCHAR",false));
        orderAtts.add(new Attribute("O_CLERK","VARCHAR",false));
        orderAtts.add(new Attribute("O_SHIPPRIORITY","INTEGER",false));
        orderAtts.add(new Attribute("O_COMMENT","VARCHAR",false));

        Table orders = new Table("ORDERS", orderAtts);



        Vector<Attribute> lineitemAtts = new Vector<Attribute>();

        lineitemAtts.add(new Attribute("L_ORDERKEY","INTEGER",true,"ORDER"));
        lineitemAtts.add(new Attribute("L_PARTKEY","INTEGER",true,"PART"));
        lineitemAtts.add(new Attribute("L_SUPPKEY","INTEGER",true,"SUPPLIER"));
        lineitemAtts.add(new Attribute("L_LINENUMBER","INTEGER",false));
        lineitemAtts.add(new Attribute("L_QUANTITY","DECIMAL",false));
        lineitemAtts.add(new Attribute("L_EXTENDEDPRICE","DECIMAL",false));
        lineitemAtts.add(new Attribute("L_DISCOUNT","DECIMAL",false));
        lineitemAtts.add(new Attribute("L_TAX","DECIMAL",false));
        lineitemAtts.add(new Attribute("L_RETURNFLAG","VARCHAR",false));
        lineitemAtts.add(new Attribute("L_LINESTATUS","VARCHAR",false));
        lineitemAtts.add(new Attribute("L_SHIPDATE","DATE",false));
        lineitemAtts.add(new Attribute("L_COMMITDATE","DATE",false));
        lineitemAtts.add(new Attribute("L_RECEIPTDATE","DATE",false));

        Table lineitem = new Table("LINEITEM", lineitemAtts);


	
	Vector<Table> tables = new Vector<Table>();
	tables.add(a);
	tables.add(b);
	//tables.add(region);
	//tables.add(part);
	//tables.add(supplier);
	//tables.add(ps);
	//tables.add(cust);
	//tables.add(orders);
	//tables.add(lineitem);

	// ----------------------------------------------
	/*
	for(int index =0; index<20; index++)
	{
  	Query q = new Query(1);
  	q.buildQuery(tables, 1, 0.7, 0.2, 
  	             QoSAcqusitionRate, probabilyOfAllAttributes, 
  	             queryType.SELECT);
  	System.out.println(q.toString());
	}
	for(int index =0; index<20; index++)
  {
    Query q = new Query(1);
    q.buildQuery(tables, 1, 0.7, 0.2, 
                 QoSAcqusitionRate, probabilyOfAllAttributes, 
                 queryType.AGG);
    System.out.println(q.toString());
  }*/
	for(int index =0; index<20; index++)
  {
    Query q = new Query(1);
    q.buildQuery(tables, 2, 0.7, 0.2, 
                 QoSAcqusitionRate, probabilyOfAllAttributes, 
                 queryType.JOIN);
    System.out.println(q.toString()+";");
  }
	
	
    }
}
