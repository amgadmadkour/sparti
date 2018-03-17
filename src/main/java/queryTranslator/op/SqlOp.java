package queryTranslator.op;

import java.util.ArrayList;
import java.util.Map;

import queryTranslator.SqlOpVisitor;


/**
 * @author Antony Neu
 */
public interface SqlOp {
	Map<String, String[]> getSchema();

	String getResultName();

	void setResultName(String _resultName);

	boolean getExpandMode();

	void setExpandMode(boolean _expandPrefixes);

	void visit(SqlOpVisitor sqlOpVisitor);

}
