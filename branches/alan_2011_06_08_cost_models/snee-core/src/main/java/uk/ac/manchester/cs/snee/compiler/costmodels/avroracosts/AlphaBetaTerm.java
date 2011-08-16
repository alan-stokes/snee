package uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * A single term in the Alpha Beta Expression.
 * @author Christian Brenninkmeijer and Ixent Galpin
 *
 */
public class AlphaBetaTerm {

  /** Numerator part of the coefficient. */
  private double numerator;
  /** Denominator part of the coefficient. */
  private double denominator;
  /** Exponent of Alpha. */
  private double alphaExponent; 
  /** Exponent of Beta. */
  private double betaExponent;
  
  /** Format for showing decimal numbers */
    private final DecimalFormat df = new DecimalFormat("0.000000");

  /** Constant only constructor.
   * @param theNumerator The constant part of term.
   */
  public AlphaBetaTerm(final double theNumerator) {
    this(theNumerator, 1, 0, 0);
  }

  /**
   * Constructor.
   * @param theNumerator The Constant part of term.
   * @param theAlphaExponent The Exponent for Alpha.
   * @param theBetaExponent The Exponent for Beta.
   */
  public AlphaBetaTerm(final double theNumerator, 
      final double theAlphaExponent, final double theBetaExponent) {
    this(theNumerator, 1, theAlphaExponent, theBetaExponent);
  }

  /**
   * Constructor.
   * @param theNumerator Numerator part of the Constant part of term.
   * @param theDenominator Denominator part of the constant.
   * @param theAlphaExponent The Exponent for Alpha.
   * @param theBetaExponent The Exponent for Beta.
   */
  public AlphaBetaTerm(final double theNumerator, 
      final double theDenominator, final double theAlphaExponent,
      final double theBetaExponent) {
    assert (theDenominator != 0);
    numerator = theNumerator;
    denominator = theDenominator;
    alphaExponent = theAlphaExponent;
    betaExponent = theBetaExponent;
  }

  /**
   * Deep clone method.
   * @return A deep copy of this term.
   */
  public final AlphaBetaTerm clone() {
    return new AlphaBetaTerm(numerator, denominator, 
      alphaExponent, betaExponent);
  }
  /**
   * Adds a constant to this term.  Stores the result in this term.
   * Only valid if this term is a constant 
   * where both Alpha and Beta Exponent equal zero.
   * @param constant Value to be added.
   */
  protected final void add(final double constant) {
    assert (alphaExponent == 0);
    assert (betaExponent == 0);
    if (this.denominator == 1) {
      numerator += constant;
    } else {
      this.numerator += (constant * this.denominator);
    }
  }
  
  /**
   * Adds a term to this term.  Stores the result in this term.
   * Only valid if both terms have the same alpha and beta exponents.
   * @param term Term to be added into this term.
   */
  protected final void add(final AlphaBetaTerm term) {
    assert (this.alphaExponent == term.alphaExponent);
    assert (this.betaExponent == term.betaExponent);
    if (this.denominator == term.denominator) {
      numerator += term.numerator;
    } else {
      this.numerator = ((this.numerator * term.denominator) 
          + (term.numerator * this.denominator));
      this.denominator = this.denominator * term.denominator;   
    }
  }
  
  /**
   * Adds two terms creating a third.
   * Original terms are not modified.
   * @param first Term to add.
   * @param second Term to add.
   * @return Result of the addition.
   */ 
  protected static AlphaBetaTerm add(final AlphaBetaTerm first, 
      final AlphaBetaTerm second) {
    assert (first.alphaExponent == second.alphaExponent);
    assert (first.betaExponent == second.betaExponent);
    if (first.denominator == second.denominator) {
      return new AlphaBetaTerm(first.numerator + second.numerator, 
        first.denominator, first.alphaExponent, first.betaExponent);
    }
    return new AlphaBetaTerm(first.numerator
      + (first.numerator / first.denominator * second.numerator),
      first.denominator, first.alphaExponent, first.betaExponent);
  }

  /**
   * Subtracts one term from another creating a third.
   * Original terms are not modified.
   * @param first Term to subtract from.
   * @param second Term to subtract.
   * @return Result of the subtraction.
   */ 
  protected static AlphaBetaTerm subtract(final AlphaBetaTerm first, 
      final AlphaBetaTerm second) {
    assert (first.alphaExponent == second.alphaExponent);
    assert (first.betaExponent == second.betaExponent);
    if (first.denominator == second.denominator) {
      return new AlphaBetaTerm(first.numerator - second.numerator, 
        first.denominator, first.alphaExponent, first.betaExponent);
    }
    return new AlphaBetaTerm(first.numerator
      - (first.numerator / first.denominator * second.numerator),
      first.denominator, first.alphaExponent, first.betaExponent);
  }

  /**
   * Provides a term which is the negative of this term.
   * Such that this + newTerm = 0  
   * @return negative term.
   */
  protected final AlphaBetaTerm getNegative() {
    return new AlphaBetaTerm(-this.numerator, this.denominator,
        this.alphaExponent, this.betaExponent);
  }
  
  /**
   * Subtract a constant from this term.  Stores the result in this term.
   * Only valid if this term is a constant 
   * where both Alpha and Beta Exponent equal zero.
   * @param constant Value to be subtracted.
   */
  protected final void subtract(final double constant) {
    if (this.denominator == 1) {
      numerator -= constant;
    } else {
      this.numerator -= (constant * this.denominator);
    }
  }
  
  /**
   * Subtracts a term from this term.  Stores the result in this term.
   * Only valid if both terms have the same alpha and beta exponents.
   * @param term Term to be added into this term.
   */
  protected final void subtract(final AlphaBetaTerm term) {
    assert (this.alphaExponent == term.alphaExponent);
    assert (this.betaExponent == term.betaExponent);
    //term.denominator known to be not zero;
    if (this.denominator == term.denominator) {
      numerator -= term.numerator;
    } else {
      this.numerator -= 
        (term.numerator / term.denominator * this.numerator);
    }
  }

  /**
   * Multiplies this term by a constant.  Stores the result in this term.
   * @param constant Value that this term is to be multiplied by.
   */
  protected final void multiplyBy(final double constant) {
    numerator = numerator * constant;
  }
  
  /**
   * Multiplies this term by another term.  Stores the result in this term.
   * @param term Term to multiply this term with.
   */
  protected final void multiplyBy(final AlphaBetaTerm term) {
    this.numerator = this.numerator * term.numerator;
    this.denominator = this.denominator * term.denominator;
    this.alphaExponent = this.alphaExponent + term.alphaExponent;
    this.betaExponent = this.betaExponent + term.betaExponent;
  }

  /**
   * Multiplies a term and a constant creating a new term.
   * Original term is not modified.
   * @param first Term to multiply.
   * @param second Constant.
   * @return Result of the multiplication.
   */
  protected static AlphaBetaTerm multiplyBy(final AlphaBetaTerm first, 
      final double second) {
    return new AlphaBetaTerm(first.numerator * second, 
      first.denominator, first.alphaExponent, first.betaExponent);
  } 

  /**
   * Multiplies two terms creating a third.
   * Original terms are not modified.
   * @param first Term to multiply.
   * @param second Term to multiply.
   * @return Result of the multiplication.
   */
  protected static AlphaBetaTerm multiplyBy(final AlphaBetaTerm first, 
      final AlphaBetaTerm second) {
    return new AlphaBetaTerm(first.numerator * second.numerator, 
      first.denominator * second.denominator,
      first.alphaExponent + second.alphaExponent,
      first.betaExponent + second.betaExponent);
  } 

  /**
   * Divides a term by a constant.
   * Original term is not modified.
   * @param first Term to divide.
   * @param second constant to divide by.
   * @return Result of the division.
   */
  public static AlphaBetaTerm divideBy(final AlphaBetaTerm first, 
      final double second) {
    return new AlphaBetaTerm(first.numerator, 
      first.denominator * second,
      first.alphaExponent, first.betaExponent);
  } 

  /**
   * Divides one term by another creating a third.
   * Original terms are not modified.
   * @param first Term to divide.
   * @param second Term to divide by.
   * @return Result of the division.
   */
  protected static AlphaBetaTerm divideBy(final AlphaBetaTerm first, 
      final AlphaBetaTerm second) {
    return new AlphaBetaTerm(first.numerator * second.denominator, 
      first.denominator * second.numerator,
      first.alphaExponent - second.alphaExponent,
      first.betaExponent - second.betaExponent);
  } 

  /**
   * Divides this term by a constant.  Stores the result in this term.
   * Only valid if the constant != 0;
   * @param constant Value that this term is to be divided by.
   */
  public final void divideBy(final double constant) {
    assert (constant != 0);
    this.denominator = this.denominator * constant;
  }
  
  /**
   * Divide this term by another term.  Stores the result in this term.
   * Only valid if term != 0 (specifically, if numerator != 0)
   * @param term Term that this term is to be divided by.
   */
  protected final void divideBy(final AlphaBetaTerm term) {
    assert (this.numerator != 0);
    this.numerator = this.numerator * term.denominator;
    this.denominator = this.denominator * term.numerator;
    this.alphaExponent = this.alphaExponent - term .alphaExponent;
    this.betaExponent = this.betaExponent - term.betaExponent;
  }
  
  /** 
   * Getter.
   * @return Numerator part of term.
   */
  protected final double getNumerator() {
    return numerator;
  }
  
  //CB Not sure we should allow setters.
  /**
   * Setter.
   * @param theNumerator New Numerator value.
   */
  protected final void setNumerator(final double theNumerator) {
    this.numerator = theNumerator;
  }
  
  /**
   * Getter.
   * @return The denominator part of the term.
   */
  protected final double getDenominator() {
    return denominator;
  }
  
  /**
   * Setter.
   * @param theDenominator New denominator value. 
   */
  protected final void setDenominator(final double theDenominator) {
    this.denominator = theDenominator;
  }
  
  /**
   * Getter.
   * @return The Exponent for alpha.
   */
  protected final double getAlphaExponent() {
    return alphaExponent;
  }
  
  /**
   * Setter.
   * @param theAlphaExponent New exponent for alpha.
   */
  protected final void setAlphaExponent(final double theAlphaExponent) {
    alphaExponent = theAlphaExponent;
  }
  
  /**
   * Getter.
   * @return The exponent for beta.
   */
  protected final double getBetaExponent() {
    return betaExponent;
  }
  
  /**
   * Setter.
   * @param theBetaExponent New exponent for beta.
   */
  protected final void setBetaExponent(final double theBetaExponent) {
    betaExponent = theBetaExponent;
  } 
  
  /**
   * Check to see if the term is negative.
   * @return True If either denominator or numerator 
   * (but not both) is smaller than zero 
   * and therefore the term is negative.
   */
  protected final boolean hasNegativeCoeffcient() {
    if (this.denominator < 0) {
      return (this.numerator > 0);
    } else {
      return (this.numerator < 0);
    }
  }
  
  /**
   * Checks whether the current term is a valid monomial.
   * As far as the CVX Matlab-based modelling system is concerned,
   * a monomial has the form: cx_1^{a_1}...x_n^{a_n} where:
   * c > 0 and
   * a_i is a member of the set of real numbers.
   *
   * @return whether the term is a monomial or not
   */
  public final boolean isValidMonomial() {
    return this.hasNegativeCoeffcient() && !this.isZero();
  }
  
  /**
   * Check to see if the term equals zero.
   * @return True If either numerator is zero 
   * and therefore the term is negative.
   */
  protected final boolean isZero() {
    return (this.numerator == 0);
  }
    
  /**
   * Checks if two terms have the same exponents.
   * @param other Term to compare with
   * @return True if and only if both terms 
   *    have the same alpha and beta exponents.
   */
  protected final boolean hasSameExponents(final AlphaBetaTerm other) {
    if (this.alphaExponent != other.alphaExponent) {
      return false;
    }
    if (this.betaExponent != other.betaExponent) {
      return false;
    }
    return true;
  }

  /**
   * Returns a string representation of a double, removing the ".0" after the
   * decimal point. 
   * @param d the double to be converted to a string
   * @return the string representing the double
  */
    private static String prettyPrint(final double d) {
      final String s = new Double(d).toString();
      if (s.endsWith(".0")) {
          return s.replace(".0", "");
      } else {
          return s;
      }
    }
  
  /** 
   * Returns a string representing the term in the format used by the CVX 
   * geometric programming solver in Matlab. 
   * @return the string representation in CVX format.
   */
  protected final String toCVXString() {
    StringBuffer sb = new StringBuffer();
    
    if (this.numerator == 0) {
      return "";
    }
    
    //Coefficient (as decimal)
    if (!(this.numerator == this.denominator && 
        (this.alphaExponent != 0 || this.betaExponent != 0))) {
      String coeffStr = prettyPrint(this.numerator / this.denominator);
      sb.append(coeffStr);
      
      if (this.alphaExponent != 0 || this.betaExponent != 0) {
        sb.append("*");
      }
    }
    
    //Alpha part of the term
    if (this.alphaExponent != 0) {
      String alphaExpStr = "";
      if (this.alphaExponent != 1) {
        alphaExpStr = "^(" + prettyPrint(this.alphaExponent) + ")";
      }     
      sb.append("alpha" + alphaExpStr);
    }

    if (this.betaExponent != 0 && this.alphaExponent != 0) {
      sb.append("*");
    }
    
    //Beta part of the term
    if (this.betaExponent != 0) {
      String betaExpStr = "";
      if (this.betaExponent != 1) {
        betaExpStr = "^(" + prettyPrint(this.betaExponent) + ")";
      }     
      sb.append("beta" + betaExpStr);
    }
    
    return sb.toString();
  }
  
  /**
   * Returns a string representing the term in the in Latex.  
   * @param coeffAsDecimal Specifies whether coefficient should be 
   * displayed as a fraction or decimal.
   * @return the string representation in latex format.
  */
  private String toLatexString(final boolean coeffAsDecimal) {
    StringBuffer numeratorSB = new StringBuffer();
    StringBuffer denominatorSB = new StringBuffer();

    if (this.numerator == 0) {
      return "";  //If you want zero terms to be displayed return "0" instead of empty string here
    }
    
    //Coefficient
    if (!(this.numerator == this.denominator && 
        (this.alphaExponent != 0 || this.betaExponent != 0))) {
      //Coefficient as decimal
      if (coeffAsDecimal) {
        numeratorSB.append(prettyPrint(this.numerator / this.denominator));
      } else { //Coefficient as fraction  
        numeratorSB.append(prettyPrint(this.numerator));
        if (this.denominator != 1) {
          denominatorSB.append(prettyPrint(this.denominator));
        }
      }   
    }       
    
    //Alpha part of the term
    if (this.alphaExponent != 0) {
      String alphaExpStr = "";
      if (Math.abs(this.alphaExponent) != 1) {
        alphaExpStr = "^{" 
          + prettyPrint(Math.abs(this.alphaExponent)) + "}";
      }
      if (this.alphaExponent > 0) {
        numeratorSB.append("\\alpha" + alphaExpStr);
      } else {
        denominatorSB.append("\\alpha" + alphaExpStr);
      }
    }

    //Beta part of the term
    if (this.betaExponent != 0) {
      String betaExpStr = "";
      if (Math.abs(this.betaExponent) != 1) {
        betaExpStr = "^{" 
          + prettyPrint(Math.abs(this.betaExponent)) + "}";
      }
      if (this.betaExponent > 0) {
        numeratorSB.append("\\beta" + betaExpStr);
      } else {
        denominatorSB.append("\\beta" + betaExpStr);
      }
    }
    
    StringBuffer sb = new StringBuffer();
    if (denominatorSB.length() == 0) {
      sb.append(numeratorSB);
    } else {
      sb.append("\\frac{");
      sb.append(numeratorSB);
      sb.append("}{");
      sb.append(denominatorSB);
      sb.append("}"); 
    }
    
    return sb.toString();   
  }
  
  /**
   * Returns a string representing the term in the in Latex. The coefficient 
   * may be displayed as a fraction if the numerator > 1.
   * @return the string representation in latex format.
   */
  public final String toLatexString() {
    return toLatexString(false);
  }
  
  /** 
   * Returns a string representing the term in the in Latex. The coefficient 
   * is displayed as a decimal.
   * @return the string representation in latex format.
   */
  protected final String toDecimalLatexString() {
    return toLatexString(true);
  }
  
  
  /**
   * Given values for the variables, substitutes these values into the term and
   * evaluates it.
   * @param alpha The value for the alpha variable
   * @param beta The value for the beta variable
   * @return The result of evaluating the term
   */
  protected final double evaluate(double alpha, double beta) {
    return (this.numerator / this.denominator) * 
      Math.pow(alpha, this.alphaExponent) * 
      Math.pow(beta, this.betaExponent);
  }
  
  /**
   * Testing funtion.
   * @param args No Testing parameters.
   * @throws IOException Never happens.
   */
  public static void main(final String[] args) throws IOException {
    AlphaBetaTerm test = new AlphaBetaTerm(1, 1, 0);
    System.out.println(test.toLatexString());
      double numb = 10;
      String rptNumb;
      DecimalFormat df = new DecimalFormat("0.000000");
      rptNumb = df.format(numb);
      System.out.println(numb+" "+rptNumb+"\n");
  }

} 
