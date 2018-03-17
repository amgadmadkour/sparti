package edu.purdue.sparti.data.parsers.querylogs;

import java.net.URLDecoder;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by Amgad Madkour
 */
public class QueryExtractor {

	public String extractQuery(String rawEntry) {

		try {
			String logentry = URLDecoder.decode(rawEntry, "UTF-8");
			logentry = logentry.replaceAll("\\n", " ");
			logentry = logentry.replaceAll("\\r", " ");
			Pattern p = Pattern.compile("^(.+?)query=(.+?)(\\&.*?)?\"$");
			Matcher m = p.matcher(logentry);
			if (m.matches()) {
				String query = m.group(2);
				query = query.replaceAll("\\+", " ");
				query = query.replaceAll("  ", " ");
				query = query.replaceAll("SELECT (.+?) WHERE", "SELECT * WHERE");
				//Complete the query with missing prefixes so that they can be parsed correctly
				query = "PREFIX soic: <http://rdfs.org/sioc/ns#> " + query;
				query = "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> " + query;
				query = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " + query;
				query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " + query;
				query = "PREFIX rdfa: <http://www.w3.org/ns/rdfa#> " + query;
				query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " + query;
				query = "PREFIX owl: <http://www.w3.org/2002/07/owl#> " + query;
				query = "PREFIX dbp: <http://dbpedia.org/property/> " + query;
				query = "PREFIX dbpprop: <http://dbpedia.org/property/> " + query;
				query = "PREFIX dbo: <http://dbpedia.org/ontology/> " + query;
				query = "PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> " + query;
				query = "PREFIX dbr: <http://dbpedia.org/resource/> " + query;
				query = "PREFIX dbr: <http://dbpedia.org/resource/> " + query;
				return query;
			}
			return null;
		} catch (Exception exp) {
			return null;
		}
	}

}
