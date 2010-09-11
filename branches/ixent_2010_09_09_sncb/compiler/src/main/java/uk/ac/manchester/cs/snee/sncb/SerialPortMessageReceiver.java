package uk.ac.manchester.cs.snee.sncb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.evaluator.types.EvaluatorAttribute;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.sncb.tos.CodeGenUtils;

import net.tinyos.message.Message;
import net.tinyos.message.MoteIF;
import net.tinyos.packet.BuildSource;
import net.tinyos.util.PrintStreamMessenger;

//Based on TinyOS MsgReader.java class
public class SerialPortMessageReceiver extends Observable 
implements net.tinyos.message.MessageListener {

	  private MoteIF moteIF;
	  
	  private DeliverOperator delOp;
	  
	  public SerialPortMessageReceiver(String source, DeliverOperator delOp) throws Exception {
	    if (source != null) {
	      moteIF = new MoteIF(BuildSource.makePhoenix(source, PrintStreamMessenger.err));
	    }
	    else {
	      moteIF = new MoteIF(BuildSource.makePhoenix(PrintStreamMessenger.err));
	    }
	    this.delOp = delOp;
	  }

	  public void start() {
	  }
	  
	  @Override
	  public void messageReceived(int to, Message message) {
	    long t = System.currentTimeMillis();
	    //    Date d = new Date(t);
	    
	    System.out.print("" + t + ": ");
	    System.out.println(message);

	    //This bit of code is based on http://java.sun.com/developer/technicalArticles/ALT/Reflection/
	    Class msgClass = message.getClass();
	    try {
	    	//TODO: Populate this array with output
			List<Output> resultList = new ArrayList<Output>();
	    	//Use to get number of tuples in each message, this is the number of tuples to notify
	    	//the result collector about...
			Method meth = msgClass.getMethod("numElements_tuples_evalEpoch", new Class[0]);
			Integer tuplesPerMessage = (Integer) meth.invoke(message, new Object[0]);
			for (int i=0; i<tuplesPerMessage; i++) {
				for (Attribute attr : this.delOp.getAttributes()) {
					//invoke getElementXXXXXX method for every field in every tuple
					//iterate over arributes of deliver operator and derive 
					//the name of the method by using getNescName
					String nesCAttrName = CodeGenUtils.getNescAttrName(attr);
					String methodName = "getElement_tuples_"+nesCAttrName;
				    Class paramTypes[] = new Class[1];
				    paramTypes[0] = Integer.TYPE;
				    meth = msgClass.getMethod(methodName, paramTypes);
					Object argList[] = new Object[1];
					argList[0] = new Integer(i);
					Object retObj = meth.invoke(message, argList);
					Integer data = (Integer)retObj;
					
					String extentName = ""; //FIXME: !!!
					String attrName = attr.getAttributeName();
					AttributeType attrType = attr.getType();
					
					EvaluatorAttribute evalAttr = new EvaluatorAttribute(extentName, attrName, attrType, data);
				}
				Tuple newTuple = new Tuple(); //TODO: XXX
				//TaggedTuple newTuple; //TODO
				
			}



			//TODO: make the ResultStore an observer of this object.
			if (!resultList.isEmpty()) {
				setChanged();
				notifyObservers(resultList);
			}			
	    } catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SchemaMetadataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TypeMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	  }

	  protected void addMsgType(Message msg) {
	    moteIF.registerListener(msg, this);
	  }

}
