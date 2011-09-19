package uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

/**
 * A collection of alpha beta terms that make up an expression.
 * @author Christian Brenninkmeijer and Ixent Galpin
 *
 */
public final class AlphaBetaExpression {
  //CB Name could change just used a different one from existing for now.

  /** Collection of terms making up this expressions. */
  private Vector<AlphaBetaTerm> termsCollection = new Vector<AlphaBetaTerm>();
    
    /**
   * Constructor that creates a zero expression.
   */
  public AlphaBetaExpression() {
  }
  
  /**
   * Constructor that creates an expression with only a constant term.
   * @param constant Value to set expression to.
   */
  public AlphaBetaExpression(final double constant) {
    addConstant(constant);
  }
  
  /**
   * Constructor that creates an expression containing 
   *    a beta term and a constant term.
   * @param betaCoefficient Coefficient of the beta term 
   *    to be added to expression.
   * @param constant Value of the constant term.
   */
  public AlphaBetaExpression(final double betaCoefficient, 
      final double constant) {
    addBetaTerm(betaCoefficient);
    addConstant(constant);
  }

  /**
   * Constructor that creates an expression containng an alpha*beta, beta and
   * constant term.
   * @param alphaBetaCoefficient Coefficient of the alpha*beta term 
   *    to be added to expression.
   * @param betaCoefficient Coefficient of the beta term 
   *    to be added to expression.
   * @param constant Value of the constant term.
   */
  public AlphaBetaExpression(final double alphaBetaCoefficient, 
      final double betaCoefficient, final double constant) {
    addAlphaBetaTerm(alphaBetaCoefficient);
    addBetaTerm(betaCoefficient);
    addConstant(constant);
  }
  
  /**
   * Constructor that creates an expression containing an alpha*beta, beta and
   * constant term.
   * @param alphaBetaCoefficient Coefficient of the alpha*beta term 
   *    to be added to expression.
   * @param alphaCoefficient Coefficient of the alpha term 
   *    to be added to expression.
   * @param betaCoefficient Coefficient of the beta term 
   *    to be added to expression.
   * @param constant Value of the constant term.
   */
  public AlphaBetaExpression(final double alphaBetaCoefficient, 
      final double alphaCoefficient, final double betaCoefficient, 
      final double constant) {
    addAlphaBetaTerm(alphaBetaCoefficient);
    addAlphaTerm(alphaCoefficient);
    addBetaTerm(betaCoefficient);
    addConstant(constant);
  }
  
  /**
   * Deep Clone.
   * Returns a copy of this expression 
   * with all terms being copied of the original terms. 
   * @return A Deep copy.
   */
  public AlphaBetaExpression clone() {
    AlphaBetaExpression result = new AlphaBetaExpression();
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      result.add(termsIter.next().clone());
    }
    return result;
  }

  /** 
   * Adds a constant to the current expression.
   * If another constant term exists the two constants are added together,
   * thus combining them into a single term.
   * Otherwise the constant added to the collection. 
   * @param constant Constant to be added to expression.
   */
  public void add(final double constant) {
    AlphaBetaTerm newTerm = new AlphaBetaTerm(constant);
    add(newTerm);
  }

  /** 
   * Adds a term to the current expression.
   * If another term exists with the same exponents 
   * the two terms are combined into a single term,
   * thus simplifying the expression.
   * Otherwise the new term is added to the collection. 
   * @param newTerm Term to be added to expression.
   */
  public void add(final AlphaBetaTerm newTerm) {
    if (newTerm.isZero()) {
      return;
    }
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      AlphaBetaTerm existingTerm = termsIter.next();
      if (existingTerm.hasSameExponents(newTerm)) {
        existingTerm.add(newTerm);
        if (existingTerm.isZero()) {
          termsCollection.remove(existingTerm);
        }
        return;
      }
    }
    termsCollection.add(newTerm);
  }
  
  /**
   * Adds two expressions.  The resulting is returned as a third expression.
   * @param first Expression to add
   * @param second Expression to Add
   * @return Result of the addition.
   */
  public static AlphaBetaExpression add(final AlphaBetaExpression first, 
    final AlphaBetaExpression second) {
    //Use clone method to add in all the first values.
    AlphaBetaExpression result = first.clone();
    //Add in all the second values;
    Iterator<AlphaBetaTerm> secondIter = second.termsCollection.iterator();
    while (secondIter.hasNext()) {
      result.add(secondIter.next());
    }
    return result;
  }
  
  /** 
   * Adds another expression to this expression.  The result is stored in 
   * the current expression.
   * In cases where both expressions have a term 
   *    with the same alpha and beta exponents,
   * these are simplified into a single term.
   * Terms with a zero value are not removed.
   * @param other Expression to be added.
   */
  public void add(final AlphaBetaExpression other) {
    Iterator<AlphaBetaTerm> termsIter = other.termsCollection.iterator();
    while (termsIter.hasNext()) {
      add(termsIter.next());
    }
  }
  
  /**
   * Same as Add(constant) function.
   * Given second name for code readability.
   * 
   * @param constant Value to add in as a constant.
   */
  public void addConstant(final double constant) {
    add(constant);
  }

  /** 
   * Adds an alpha*beta term to the expression.  The result is stored
   * in the current expression.
   * In cases where the result has more than one term 
   *    with the same alpha and beta exponents,
   * these are simplified into a single term.
   * Otherwise a new term (constant*alpha^1*beta^1) 
   *    is added to the expression. 
   * @param alphaBetaCoefficient AlphaBeta Value to be added 
   *    to expression.
   */
  public void addAlphaBetaTerm(final double alphaBetaCoefficient) {
    AlphaBetaTerm newTerm = new AlphaBetaTerm(alphaBetaCoefficient, 1, 1);
    add(newTerm);
  }

  /** 
   * Adds an alpha term to the expression.  
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   *    with the same alpha and beta exponents,
   * these are simplified into a single term.
   * Otherwise a new term (constant*alpha^1*beta^0) 
   *    is added to the expression. 
   * @param alphaCoefficient Alpha Value to be added to expression.
   */
  public void addAlphaTerm(final double alphaCoefficient) {
    AlphaBetaTerm newTerm = new AlphaBetaTerm(alphaCoefficient, 1, 0);
    add(newTerm);
  }

  /** 
   * Adds a beta term to the expression.  The result is stored in the current
   * expression.
   * In cases where the result has more than one term 
   *    with the same alpha and beta exponents,
   * these are simplified into a single term.
   * Otherwise a new term (constant*alpha^0*beta^1) 
   *    is added to the expression. 
   * @param betaCoefficient Beta Value to be added to expression.
   */
  public void addBetaTerm(final double betaCoefficient) {
    AlphaBetaTerm newTerm = new AlphaBetaTerm(betaCoefficient, 0, 1);
    add(newTerm);
  }
  
  /** 
   * Subtracts a constant from the expression.  
   * The result is stored in the current expression.
   * If a constant term already exists in the expression, 
   * these are simplified into a single term.
   * Otherwise a negative constant term is added to the expression. 
   * @param constant Constant to be subtracted from expression.
   */
  public void subtract(final double constant) {
    add(-constant);
  }

  /** 
   * Subtracts a term from the expression.  
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   *    with the same alpha and beta exponents,
   * these are simplified into a single term.
   * Otherwise the new term is added to the expression. 
   * @param newTerm Term to be subtracted to expression.
   */
  public void subtract(final AlphaBetaTerm newTerm) {
    add(newTerm.getNegative());
  }
  
    /** 
   * Subtract an expression from this expression.  
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   *    with the same alpha and beta exponents,
   * these are simplified into a single term.
   * Terms with a zero value are not removed.
   * @param other Expression to be added in.
   */
  public void subtract(final AlphaBetaExpression other) {
    Iterator<AlphaBetaTerm> termsIter = other.termsCollection.iterator();
    while (termsIter.hasNext()) {
      subtract(termsIter.next());
    }
  }

  /**
   * Subtracts one expression Adds two expressions.  
   * The resulting is returned as a third expression.
   * @param first Expression to subtract from
   * @param second Expression to subtract.
   * @return Result of the addition.
   */
  public static AlphaBetaExpression subtract(final AlphaBetaExpression first, 
      final AlphaBetaExpression second) {
    //Use clone method to add in all the first values.
    AlphaBetaExpression result = first.clone();
    //Add in all the second values;
    Iterator<AlphaBetaTerm> secondIter = second.termsCollection.iterator();
    while (secondIter.hasNext()) {
      result.subtract(secondIter.next());
    }
    return result;
  }
  
  /**
   * Subtracts one expression Adds two expressions.  
   * The resulting is returned as a third expression.
   * @param first Expression to subtract from
   * @param second Expression to subtract.
   * @return Result of the addition.
   */
  public static AlphaBetaExpression subtract(final AlphaBetaExpression first, 
      final double second) {
    //Use clone method to add in all the first values.
    AlphaBetaExpression result = first.clone();
    result.subtract(second);
    return result;
  }

  /** 
   * Multiply the expression by a constant.
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term.
   * @param constant Constant to be added to expression.
   */
  public void multiplyBy(final double constant) {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      termsIter.next().multiplyBy(constant);
    }
  }

  /** 
   * Multiply the expression by a given term.
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term.
   * @param newTerm Term that the expression is to be multiplied by.
   */
  public void multiplyBy(final AlphaBetaTerm newTerm) {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      termsIter.next().multiplyBy(newTerm);
    }
  }
  
  /** 
   * Multiply the expression by another expression.
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term.
   * @param other Expression that the expression is to be multiplied by.
   */
  public void multiplyBy(final AlphaBetaExpression other) {
    termsCollection = multiplyBy(this, other).termsCollection;
  }
  
  /** 
   * Multiply an expression and a constant, and return the result.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term. 
   * @param first Expression to multiply 
   * @param second constant to multiply.
   * @return The product of both.
   */
  public static AlphaBetaExpression multiplyBy(
      final AlphaBetaExpression first, final double second) {
    Iterator<AlphaBetaTerm> firstIter = first.termsCollection.iterator();
    AlphaBetaExpression result = new AlphaBetaExpression();
    //If constant is zero return an empty expression.
    if (second == 0) {
      return result; 
    }
    while (firstIter.hasNext()) {
      AlphaBetaTerm firstTerm = firstIter.next();
      AlphaBetaTerm multiple = 
          AlphaBetaTerm.multiplyBy(firstTerm, second);
      result.add(multiple);
    }
    return result;
  }

  /** 
   * Multiply an expression and a term, and return the result.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term. 
   * @param first Expression to multiply 
   * @param second Term to multiply.
   * @return The product of both.
   */
  public static AlphaBetaExpression multiplyBy(
      final AlphaBetaExpression first, final AlphaBetaTerm second) {
    Iterator<AlphaBetaTerm> firstIter = first.termsCollection.iterator();
    AlphaBetaExpression result = new AlphaBetaExpression();
    while (firstIter.hasNext()) {
      AlphaBetaTerm firstTerm = firstIter.next();
      AlphaBetaTerm multiple = 
          AlphaBetaTerm.multiplyBy(firstTerm, second);
      result.add(multiple);
    }
    return result;
  }

  /** 
   * Multiply two expressions, and return the result.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term. 
   * @param first Expression to multiply 
   * @param second Other Expression to be added in.
   * @return The product of both expressions.
   */
  public static AlphaBetaExpression multiplyBy(
      final AlphaBetaExpression first, final AlphaBetaExpression second) {
    Iterator<AlphaBetaTerm> firstIter = first.termsCollection.iterator();
    AlphaBetaExpression result = new AlphaBetaExpression();
    while (firstIter.hasNext()) {
      AlphaBetaTerm firstTerm = firstIter.next();
      Iterator<AlphaBetaTerm> secondIter = second.termsCollection.iterator();
      while (secondIter.hasNext()) {
        AlphaBetaTerm multiple = 
          AlphaBetaTerm.multiplyBy(firstTerm, secondIter.next());
        result.add(multiple);
      }
    }
    return result;
  }

  /** 
   * Divide the expression by a constant.
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term.
   * @param constant Constant that the expression is to be divided by.
   */
  public void divideBy(final double constant) {
    if (constant == 0) {
      throw new ArithmeticException("Divide by zero");
    }
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      termsIter.next().divideBy(constant);
    }
  }

  /** 
   * Divide every term in the expression by this term.
   * The result is stored in the current expression.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term.
   * @param newTerm Term that the expression is to be divided by.
   */
  public void divideBy(final AlphaBetaTerm newTerm) {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    if (newTerm.isZero()) {
      throw new ArithmeticException("Divide by zero");
    }
    while (termsIter.hasNext()) {
      termsIter.next().divideBy(newTerm);
    }
  }
  
  /** 
   * Divide an expression by a term, and return the result.
   * In cases where the result has more than one term 
   * with the same alpha and beta exponents,
   * these are simplified into a single term. 
   * @param first Expression to divide 
   * @param second Term to divide by.
   * @return The expression divided by the term.
   */
  public static AlphaBetaExpression divideBy(final AlphaBetaExpression first, 
      final AlphaBetaTerm second) {
    Iterator<AlphaBetaTerm> firstIter = first.termsCollection.iterator();
    AlphaBetaExpression result = new AlphaBetaExpression();
    while (firstIter.hasNext()) {
      AlphaBetaTerm firstTerm = firstIter.next();
      AlphaBetaTerm multiple = 
          AlphaBetaTerm.divideBy(firstTerm, second);
      result.add(multiple);
    }
    return result;
  }

  /** 
   * Divides AlphaBeta expression by 1*alpha*Beta. 
   * @param expr The Expression that is to be divided by 1*alpha*beta.
   * @return the result of the computation.
   */
  public static AlphaBetaExpression divideByAlphaBeta(
      final AlphaBetaExpression expr) {   
    AlphaBetaTerm alphaBetaTerm = new AlphaBetaTerm(1, 1, 1);
    return AlphaBetaExpression.divideBy(expr, alphaBetaTerm);
  }


  /** 
   * Divides AlphaBeta expression by a constant. 
   * @param expr The Expression that is to be divided by the costant.
   * @return the result of the computation.
   */
  public static AlphaBetaExpression divideByConstant(
      final AlphaBetaExpression expr, final double constant) {    
    AlphaBetaTerm alphaBetaTerm = new AlphaBetaTerm(constant, 0, 0);
    return AlphaBetaExpression.divideBy(expr, alphaBetaTerm);
  }
  
  /** 
   * Divides AlphaBeta expression by 1*alpha*Beta. 
   * @param expr The Expression that is to be divided by 1*alpha*beta.
   * @return the result of the computation.
   */
  public void divideByAlphaBeta() {
    AlphaBetaTerm alphaBeta = new AlphaBetaTerm(1, 1, 1);
    this.divideBy(alphaBeta);
  }
  
  /**
   * Checks whether the current expression is a valid monomial.
   * As far as the CVX Matlab-based modelling system is concerned,
   * a monomial has the form: cx_1^{a_1}...x_n^{a_n} where:
   * c > 0 and
   * a_i is a member of the set of real numbers.
   *
   * @return whether the expression is a monomial or not
   */
  public boolean isValidMonomial() {
    int monomialCount = 0;
    
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.isValidMonomial()) {
        monomialCount++;
      } else if (!term.isZero()) {
        return false;
      }
    }
    
    return (monomialCount == 1);
  }

  /**
   * Checks whether the current expression is a valid posynomial.
   * A posynomial is defined as the sum of 1 or more monomials.
   * 
   * @return whether the expression is a posynomial or not
   */
  public boolean isValidPosynomial() {
    int monomialCount = 0;
    
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.isValidMonomial()) {
        monomialCount++;
      } else if (!term.isZero()) {
        return false;
      }
    }
    
    return (monomialCount >= 1);    
  }

  /** The various output formats supported in this class. */
  private enum OutputFormat { CVX, Latex, DecimalLatex }; 
  
  /** 
   * Returns a string representing the expression in the format specified. 
   * @param format the output format
   * @return String representation of the expression
   **/
  private String toString(final OutputFormat format) {
    boolean first = true;
    StringBuffer sb = new StringBuffer();
    
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.isZero()) {
        continue;
      }
      if (first) {
        first = false;
      } else {
        sb.append(" + ");
      }
      String termStr;
      
      if (format == OutputFormat.CVX) {
        termStr = term.toCVXString();
      } else if (format == OutputFormat.Latex) {
        termStr = term.toLatexString();
      } else {
        termStr = term.toDecimalLatexString();
      }
      
      sb.append(termStr);
    }
    if (sb.length() == 0) {
      return "0";
    }
    return sb.toString();   
  }

  
    /** 
   * Returns a string representing the expression 
   *    in the format used by the CVX 
   * geometric programming solver in Matlab. 
   * @return the string representation in CVX format.
   */
  public String toString() {
    return this.toString(OutputFormat.CVX);
  }

  
  /**
   * TODO.
   * @param temp TODO
   */
  public void setAltLatexRep(final String temp) {
    //TODO
  }
  
  
  /**
   * Returns a string representing the expression in the in Latex. 
   * The coefficients 
   * may be displayed as a fraction if the numerator > 1.
   * @return the string representation in latex format.
   */
  public String toLatexString() {
    String latex = this.toString(OutputFormat.Latex);
    return this.toString(OutputFormat.Latex);
  }

  /**
   * Returns a string representing the expression in the in Latex. 
   * The coefficients will be displayed as decimals rather than fractions.
   * @return the string representation in latex format.
   */
  public String toDecimalLatexString() {
    return this.toString(OutputFormat.DecimalLatex);
  }

  /**
   * Given values for the variables, substitutes these values into the expression and
   * evaluates it.
   * @param alpha The value for the alpha variable
   * @param beta The value for the beta variable
   * @return The result of evaluating the expression
   */
  public double evaluate(double alpha, double beta) {
    double total = 0;
  
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      total = total + term.evaluate(alpha, beta);
    }
    
    return total;
  }
  
  public double evaluate() {
    double total = 0;
  
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      total = total + term.evaluate();
    }
    
    return total;
  }
  /**
   * Factory method
   * @return An AlphaBetaExpression with value 0 
   */
  public static AlphaBetaExpression ZERO() {
    return new AlphaBetaExpression(0, 0, 0, 0);
  }
  
  /**
   * Factory method
   * @return An AlphaBetaExpression with value 1*alpha*beta 
   */
  public static AlphaBetaExpression ALPHABETA() {
    return new AlphaBetaExpression(1, 0, 0, 0);
  }

  /**
   * Factory method
   * @return An AlphaBetaExpression with value 1*alpha 
   */
  public static AlphaBetaExpression ALPHA() {
    return new AlphaBetaExpression(0, 1, 0, 0);
  }

  /**
   * Factory method
   * @return An AlphaBetaExpression with value 1*beta 
   */
  public static AlphaBetaExpression BETA() {
    return new AlphaBetaExpression(0, 0, 1, 0);
  }
  
  /**
   * Returns the constant value in an expression.
   * If none, returns zero.
   */
  public double getConstant() {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.getAlphaExponent()==0 && term.getBetaExponent()==0) {
        return term.getNumerator() / term.getDenominator();
      }
    }
    
    return 0;   
  }
  
  
  /**
   * Returns the alpha coefficient value in an expression.
   * If none, returns zero.
   */
  public double getAlphaCoeff() {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.getAlphaExponent()==1 && term.getBetaExponent()==0) {
        return term.getNumerator() / term.getDenominator();
      }
    }
    
    return 0;   
  }
  

  /**
   * Returns the beta coefficient value in an expression.
   * If none, returns zero.
   */
  public double getBetaCoeff() {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.getAlphaExponent()==0 && term.getBetaExponent()==1) {
        return term.getNumerator() / term.getDenominator();
      }
    }
    
    return 0;   
  }
  
  /**
   * Returns the beta coefficient value in an expression.
   * If none, returns zero.
   */
  public double getBeta2Coeff() {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.getAlphaExponent()==0 && term.getBetaExponent()==2) {
        return term.getNumerator() / term.getDenominator();
      }
    }
    
    return 0;   
  }
  
  
  /**
   * Returns the alphabeta coefficient value in an expression.
   * If none, returns zero.
   */
  public double getAlphaBetaCoeff() {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.getAlphaExponent()==1 && term.getBetaExponent()==1) {
        return term.getNumerator() / term.getDenominator();
      }
    }
    
    return 0;   
  } 
  
  
  /**
   * Checks whether an expression is a beta expression (i.e., comprises just a beta
   * terms, and optionally a constant)
   * @return
   */
  public boolean isBetaExpression() {
    Iterator<AlphaBetaTerm> termsIter = termsCollection.iterator();
    
    while (termsIter.hasNext()) {
      AlphaBetaTerm term = termsIter.next();
      if (term.getAlphaExponent()!=0) {
        return false;
      }
      if (term.getBetaExponent() != 0 && term.getBetaExponent() != 1) {
        return false;
      }
    }
    
    return true;    
  }
  
  
  /**
   * Testing function.
   * @param args No Testing parameters.
   * @throws IOException Never happens.
   */
  public static void main(final String[] args) throws IOException {
    AlphaBetaExpression zero = new AlphaBetaExpression();
    System.out.println("zero");
    System.out.println(zero.toLatexString());
    AlphaBetaExpression one = new AlphaBetaExpression(1);   
    System.out.println("one");
    System.out.println(one.toLatexString());
    AlphaBetaExpression twoplus3Beta = 
      new AlphaBetaExpression(3, 2);    
    System.out.println("twoplus3Beta");
    System.out.println(twoplus3Beta.toLatexString());
    AlphaBetaExpression fiveAlphaBetafourBetaplusSix = 
      new AlphaBetaExpression(5, 4, 6);   
    System.out.println("FiveAlphaBetafourBetaplusSix");
    System.out.println(fiveAlphaBetafourBetaplusSix.toLatexString());
    System.out.println("multiple");
    System.out.println(AlphaBetaExpression.multiplyBy(twoplus3Beta, 
        fiveAlphaBetafourBetaplusSix).toLatexString());
    AlphaBetaExpression fourDoubleExpression = 
      new AlphaBetaExpression(3, -2, -7, -4);   
    System.out.println("fourDoubleExpression");
    System.out.println(fourDoubleExpression.toLatexString());
    fourDoubleExpression.subtract(1);   
    System.out.println("fourDoubleExpression - 1");
    System.out.println(fourDoubleExpression.toLatexString());
    AlphaBetaExpression oneOverAlpaBeta = 
      AlphaBetaExpression.divideByAlphaBeta(one);   
    System.out.println("oneOverAlpaBeta");
    System.out.println(oneOverAlpaBeta.toLatexString());
    System.out.println("fourDoubleExpression -1 / alphaBeta");
    System.out.println(AlphaBetaExpression.multiplyBy(
        fourDoubleExpression, oneOverAlpaBeta).toLatexString());
    System.out.println("fourDoubleExpression -1 / alphaBeta version 2");
    System.out.println(AlphaBetaExpression.divideByAlphaBeta(
        fourDoubleExpression).toLatexString());
    System.out.println("fourDoubleExpression new");
    fourDoubleExpression.multiplyBy(oneOverAlpaBeta);
    System.out.println(fourDoubleExpression.toLatexString());
    System.out.println("FiveAlphaBetafourBetaplusSix");
    System.out.println(fiveAlphaBetafourBetaplusSix.toLatexString());
    System.out.println("sum 1");
    System.out.println(AlphaBetaExpression.add(fourDoubleExpression, 
        fiveAlphaBetafourBetaplusSix).toLatexString());
    System.out.println("version 2 new fourDoubleExpression");
    fourDoubleExpression.add(fiveAlphaBetafourBetaplusSix);
    System.out.println(fourDoubleExpression.toLatexString());
    System.out.println("twoplus3Beta");
    System.out.println(twoplus3Beta.toLatexString());
    AlphaBetaExpression zero2 = 
      AlphaBetaExpression.subtract(twoplus3Beta, twoplus3Beta); 
    System.out.println("minus itself");
    System.out.println(zero2.toLatexString());
    zero2 = twoplus3Beta.clone();
    zero2.subtract(twoplus3Beta);
    System.out.println("minus itself version 2");
    System.out.println(zero2.toLatexString());
    AlphaBetaExpression addtest = new AlphaBetaExpression();
    addtest.add(twoplus3Beta);
    System.out.println("twoplus3Beta added to zero");
    System.out.println(twoplus3Beta.toLatexString());
    AlphaBetaExpression addtest2 = new AlphaBetaExpression();
    System.out.println("addtest2");
    addtest2.add(new AlphaBetaTerm(1, 0, 1));
    addtest2.add(one);
    addtest2.divideBy(2);
    System.out.println(addtest2.toLatexString());
    addtest = new AlphaBetaExpression(0, 0, 0, 0);
    System.out.println(addtest.toLatexString());    
    System.out.println(addtest.termsCollection.size());   
    addtest.add(addtest2);
    System.out.println(addtest.toLatexString());
  }

}
