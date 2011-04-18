/**
 *
 * Provided by LebLab
 *
 * @author lebiathan
 *
 */
package gr.uoa.di.ssg4e.dat.classification;

/**
 * 
 */
public enum Classifiers {
	KNN, /** K-Nearest Neighbor */
	DECISION_TREE, /** Decision Trees classification. Default for C4.5 */
	HOEFFDING_TREE, /** Decision Trees classification with Hoeffding Trees */
	LINEAR_REGRESSION; /** Linear Regression, currently being implemented */
}
