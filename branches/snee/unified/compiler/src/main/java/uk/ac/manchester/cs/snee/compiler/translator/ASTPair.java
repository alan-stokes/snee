package uk.ac.manchester.cs.snee.compiler.translator;

import antlr.collections.AST;

public class ASTPair {
	
	private AST first;
	private AST next;

	public ASTPair(AST first, AST next){
		this.first = first;
		this.next = next;
	}
	
	public AST getFirst(){
		return first;
	}

	public AST getNext(){
		return next;
	}
	
	public String toString() {
		StringBuffer output = new StringBuffer();
		if (first != null)
			output.append(first.toStringList());
		output.append(", ");
		if (next != null)
			output.append(next.toStringList());
		return output.toString();
	}
	
}
