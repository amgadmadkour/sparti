package queryTranslator.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.print.attribute.standard.SheetCollate;

//import queryTranslator.ScalaGenerator;
import queryTranslator.Tags;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.util.FmtUtils;

public class SimpleTriple implements SqlTriple {
	public Triple _t1;
	private String _tableName;
	public boolean verticalPartitioning = false;
	private int _tabs;
	private PrefixMapping _prefixMapping;
	private boolean _isDistinct = false;

	public SimpleTriple(Triple a) {
		this._t1 = a;
	}

	@Override
	public void setDistinct() {
		_isDistinct = true;
	}

	@Override
	public String getType() {
		return "simple";
	}

	@Override
	public Triple getFirstTriple() {
		return _t1;
	}

	@Override
	public Triple getSecondTriple() {
		return null;
	}

	@Override
	public SqlStatement translate() {
		Select select = new Select(this._tableName);
		select.setTabs(_tabs);
		Map<String, String> vars = new HashMap<String, String>();
		ArrayList<String> whereConditions = new ArrayList<String>();
		boolean first = true;
		Node subject = _t1.getSubject();
		Node predicate = _t1.getPredicate();
		Node object = _t1.getObject();

		if (subject.isURI() || subject.isBlank()) {
//			whereConditions.add(Tags.SUBJECT_COLUMN_NAME + " = '" + FmtUtils.stringForNode(subject,
//					this._prefixMapping).replace("http://yago-knowledge.org/resource/", "") + "'");
			whereConditions.add(Tags.SUBJECT_COLUMN_NAME + " = '" + FmtUtils.stringForNode(subject,
					this._prefixMapping) + "'");

		} else {
			vars.put(Tags.SUBJECT_COLUMN_NAME, subject.getName());
		}

		if (predicate.isURI()) {
//			this._tableName = FmtUtils.stringForNode(predicate, this._prefixMapping).replace("http://yago-knowledge.org/resource/", "").replace(":", "__");
			this._tableName = FmtUtils.stringForNode(predicate, this._prefixMapping).replace(":", "__");
			this.verticalPartitioning = true;
		} else {
			vars.put(Tags.PREDICATE_COLUMN_NAME, predicate.getName());
		}
		if (object.isURI() || object.isLiteral() || object.isBlank()) {
//			String string = FmtUtils.stringForNode(object,
//					this._prefixMapping).replace("http://yago-knowledge.org/resource/", "");
			String string = FmtUtils.stringForNode(object,
					this._prefixMapping);

			//if (object.isLiteral()) {
			//	string = "" + object.getLiteral().toString();
			//}

			whereConditions.add(Tags.OBJECT_COLUMN_NAME + " = '" + string + "'");
		} else {
			vars.put(Tags.OBJECT_COLUMN_NAME, object.getName());
		}

		ArrayList<String> varSet = new ArrayList<String>();
		for (String var : vars.keySet()) {
			select.addSelector(vars.get(var), new String[]{var});
			varSet.add(vars.get(var));
		}
		select.setVariables(varSet);

		// FROM
		select.setFrom(this._tableName);
		// WHERE
		for (String where : whereConditions) {
			select.addConjunction(where);
		}
		select.setDistinct(_isDistinct);
		return select;
	}

	@Override
	public void setTableName(String tName) {
		_tableName = tName;
	}

	@Override
	public String getTableName() {
		return _tableName;
	}

	@Override
	public void setPrefixMapping(PrefixMapping pMapping) {
		_prefixMapping = pMapping;
	}

	public ArrayList<String> getVariables() {
		ArrayList<String> res = new ArrayList<String>();
		if (_t1.getSubject().isVariable())
			res.add(_t1.getSubject().getName());
		if (_t1.getObject().isVariable())
			res.add(_t1.getObject().getName());
		if (_t1.getPredicate().isVariable())
			res.add(_t1.getPredicate().getName());
		return res;
	}

	@Override
	public Map<String, String[]> getMappings() {
		ArrayList<String> original = getVariables();
		HashMap<String, String[]> result = new HashMap<String, String[]>();
		for (String key : original) {
			result.put(key, new String[]{_tableName, key});

		}
		return result;
	}

	@Override
	public int getTabs() {
		return _tabs;
	}

	@Override
	public void setTabs(int tabs) {
		this._tabs = tabs;

	}

	@Override
	public int getNumberOfValues() {
		int result = 0;
		if (!_t1.getSubject().isVariable())
			result++;
		if (!_t1.getPredicate().isVariable())
			result++;
		if (!_t1.getObject().isVariable())
			result++;
		return result;
	}
}
