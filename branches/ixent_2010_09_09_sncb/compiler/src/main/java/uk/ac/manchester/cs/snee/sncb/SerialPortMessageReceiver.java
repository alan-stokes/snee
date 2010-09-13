package uk.ac.manchester.cs.snee.sncb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import org.apache.log4j.Logger;

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

	private Logger logger = 
		Logger.getLogger(TinyOS_SNCB.class.getName());
	
	private MoteIF moteIF;
	  
	private DeliverOperator delOp;
	
	private Message _msg;
	
	public SerialPortMessageReceiver(String source, DeliverOperator delOp) throws Exception {
		if (source != null) {
			moteIF = new MoteIF(BuildSource.makePhoenix(source, PrintStreamMessenger.err));
	    }
	    else {
	    	moteIF = new MoteIF(BuildSource.makePhoenix(PrintStreamMessenger.err));
	    }
	    this.delOp = delOp;
	}

	@Override
	public void messageReceived(int to, Message message) {
		long t = System.currentTimeMillis();
	    //    Date d = new Date(t);
	    
	    System.out.print("" + t + ": ");
	   // System.out.println(message);
	    
	    try {
			List<Output> resultList = new ArrayList<Output>();
			int tuplesPerMessage = getTuplesPerMessage(message);
			for (int i=0; i<tuplesPerMessage; i++) {
				Tuple newTuple = new Tuple();
				for (Attribute attr : this.delOp.getAttributes()) {
					EvaluatorAttribute evalAttr = getAttribute(attr, message, i);
					newTuple.addAttribute(evalAttr);
				}
				//TODO: For now, In-Network only returns tagged tuples, no windows.
				TaggedTuple newTaggedTuple = new TaggedTuple(newTuple);
				resultList.add(newTaggedTuple);
			}
			//TODO: make the ResultStore an observer of this object.
			if (!resultList.isEmpty()) {
				setChanged();
				notifyObservers(resultList);
			}			
	    } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	  }

	private int getTuplesPerMessage(Message message) 
	throws SecurityException, NoSuchMethodException, IllegalArgumentException, 
	IllegalAccessException, InvocationTargetException {
		//This bit of code is based on http://java.sun.com/developer/technicalArticles/ALT/Reflection/
		Class msgClass = message.getClass();
		Method meth = msgClass.getMethod("numElements_tuples_evalEpoch", new Class[0]);
		//Method meth = msgClass.getMethod("numElements_tuples_evalEpoch", new Class[]{Integer.TYPE});
		Integer tuplesPerMessage = (Integer) meth.invoke(message, new Object[0]);		
		//Integer tuplesPerMessage = (Integer) meth.invoke(message, new Object[]{new Integer(0)});
		return tuplesPerMessage;
	}
	
	
	/**
	 * For each attribute, deliver operator and derive getElementXXXXXX method 
	 * name and obtain value.
	 * @param attr
	 * @param message
	 * @param index
	 * @return
	 * @throws SchemaMetadataException
	 * @throws TypeMappingException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 */
	private EvaluatorAttribute getAttribute(Attribute attr, Message message, int index) 
	throws SchemaMetadataException, TypeMappingException, IllegalArgumentException, 
	IllegalAccessException, InvocationTargetException, SecurityException, NoSuchMethodException {
		if (logger.isTraceEnabled())
				logger.trace("ENTER getAttribute()");
		String nesCAttrName = CodeGenUtils.getNescAttrName(attr);
		String methodName = "getElement_tuples_"+nesCAttrName;
		Class paramTypes[] = new Class[1];
		paramTypes[0] = Integer.TYPE;
		//This bit of code is based on http://java.sun.com/developer/technicalArticles/ALT/Reflection/
		Class msgClass = message.getClass();
		Method meth = msgClass.getMethod(methodName, paramTypes);
		Object argList[] = new Object[1];
		argList[0] = new Integer(index);
		Object retObj = meth.invoke(message, argList);
		Integer data = (Integer)retObj;
			
		String extentName = ""; //FIXME: !!!
		String attrName = attr.getAttributeName();
		AttributeType attrType = attr.getType();
			
		EvaluatorAttribute evalAttr = new EvaluatorAttribute(extentName, attrName, attrType, data);
		if (logger.isTraceEnabled())
			logger.trace("ENTER getAttribute()");
		return evalAttr;
	  }
	  
	protected void addMsgType(Message msg) {
		this.moteIF.registerListener(msg, this);
		this._msg = msg;
	}
	
	public void close() {
		this.moteIF.deregisterListener(_msg, this);
	}

}
