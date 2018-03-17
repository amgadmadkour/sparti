package queryTranslator.op;

import queryTranslator.SqlOpVisitor;
import queryTranslator.sql.SqlStatement;

import com.hp.hpl.jena.shared.PrefixMapping;


/**
 * @author Alexander Schaetzle
 */


public class SqlConditional extends SqlOp2 {

	protected SqlConditional(SqlOp _leftOp, SqlOp _rightOp,
	                         PrefixMapping _prefixes) {
		super(_leftOp, _rightOp, _prefixes);
	}

	@Override
	public void visit(SqlOpVisitor sqlOpVisitor) {
	}

	@Override
	public SqlStatement translate(String name, SqlStatement left,
	                              SqlStatement right) {
		return null;
	}


}


