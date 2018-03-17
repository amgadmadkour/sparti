package queryTranslator.sql;

import java.util.ArrayList;
import java.util.Map;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;


public interface SqlTriple {

	String getType();

	Triple getFirstTriple();

	Triple getSecondTriple();

	SqlStatement translate();

	void setTableName(String tName);

	String getTableName();

	void setPrefixMapping(PrefixMapping pMapping);

	ArrayList<String> getVariables();

	Map<String, String[]> getMappings();

	int getTabs();

	void setDistinct();

	void setTabs(int tabs);

	int getNumberOfValues();
}
