package queryTranslator;


import queryTranslator.op.SqlBGP;
import queryTranslator.op.SqlDistinct;
import queryTranslator.op.SqlFilter;
import queryTranslator.op.SqlJoin;
import queryTranslator.op.SqlLeftJoin;
import queryTranslator.op.SqlOrder;
import queryTranslator.op.SqlProject;
import queryTranslator.op.SqlReduced;
import queryTranslator.op.SqlSequence;
import queryTranslator.op.SqlSlice;
import queryTranslator.op.SQLUnion;

/**
 * @author Antony Neu
 */
public interface SqlOpVisitor {

	// Operators
	void visit(SqlBGP sqlBGP);

	void visit(SqlFilter sqlFilter);

	void visit(SqlJoin sqlJoin);

	void visit(SqlSequence sqlSequence);

	void visit(SqlLeftJoin sqlLeftJoin);

	void visit(SQLUnion sqlUnion);

	// Solution Modifier
	void visit(SqlProject sqlProject);

	void visit(SqlDistinct sqlDistinct);

	void visit(SqlReduced sqlReduced);

	void visit(SqlOrder sqlOrder);

	void visit(SqlSlice sqlSlice);

}
