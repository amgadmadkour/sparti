package edu.purdue.sparti.queryprocessing.translation;


import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.optimize.*;
import edu.purdue.sparti.metadata.SpartiMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import queryTranslator.SparkTableStatistics;
import queryTranslator.SqlOpTranslator;
import queryTranslator.Tags;
import queryTranslator.op.SqlOp;
import queryTranslator.sparql.AlgebraTransformer;
import queryTranslator.sparql.BGPOptimizerNoStats;
import queryTranslator.sparql.TransformFilterVarEquality;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Amgad Madkour
 */
public class SpartiTranslator {

	private static final Logger LOG = Logger.getLogger(SpartiTranslator.class.getName());

	public SpartiTranslator(List<Map> tableStats) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		//Load the modified statistics module of S2RDF
		SparkTableStatistics.init(tableStats);
	}

	public String translate(String sparqlQuery) {

		Query query = QueryFactory.create(sparqlQuery);
		PrefixMapping prefixes = query.getPrefixMapping();
		Op sparqlOperator = Algebra.compile(query);

		//LOG.debug("PREFIX : \n" + prefixes);
		//LOG.debug("SPARQL QUERY : \n" + sparqlQuery);
		//LOG.debug("ALGEBRA : \n" + sparqlOperator.toString(prefixes));
		//Output optimized Algebra Tree to log file
//		LOG.debug("################################");
//		LOG.debug("Unoptimized Algebra Tree of Query:");
//		LOG.debug("################################");
//		LOG.debug(sparqlOperator.toString(prefixes));
//		LOG.debug("");

		//ARQ optimization of Filter conjunction
//		TransformFilterConjunction filterConjunction = new TransformFilterConjunction();
//		sparqlOperator = Transformer.transform(filterConjunction, sparqlOperator);
//
//		//ARQ optimization of Filter disjunction
//		TransformFilterDisjunction filterDisjunction = new TransformFilterDisjunction();
//		sparqlOperator = Transformer.transform(filterDisjunction, sparqlOperator);
//
//		//ARQ optimization of Filter equality
//		TransformFilterEquality filterEquality = new TransformFilterEquality();
//		sparqlOperator = Transformer.transform(filterEquality, sparqlOperator);
//
//		//Own optimization of Filter variable equality
//		TransformFilterVarEquality filterVarEquality = new TransformFilterVarEquality();
//		sparqlOperator = filterVarEquality.transform(sparqlOperator);
//
//		BGPOptimizerNoStats bgpOptimizer = new BGPOptimizerNoStats();
//		sparqlOperator = bgpOptimizer.optimize(sparqlOperator);

//		TransformFilterPlacement filterPlacement = ne   w TransformFilterPlacement();
//		sparqlOperator = Transformer.transform(filterPlacement, sparqlOperator);

//		//Output optimized Algebra Tree to log file
//		LOG.debug("################################");
//		LOG.debug("optimized Algebra Tree of Query:");
//		LOG.debug("################################");
//		LOG.debug(sparqlOperator.toString(prefixes));
//		LOG.debug("");
//
//		Tags.expandPrefixes = false;
//		Tags.optimizer = false;


		AlgebraTransformer transformer = new AlgebraTransformer(prefixes);
		SqlOp sqlOpRoot = transformer.transform(sparqlOperator);
		SqlOpTranslator translator = new SqlOpTranslator();
		String sqlScript = translator.translate(sqlOpRoot, Tags.expandPrefixes);


		return sqlScript;
	}

}
