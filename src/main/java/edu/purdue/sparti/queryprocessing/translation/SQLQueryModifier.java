package edu.purdue.sparti.queryprocessing.translation;

import edu.purdue.sparti.metadata.SpartiMetadata;
import edu.purdue.sparti.utils.Permute;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Amgad Madkour
 */
public class SQLQueryModifier {

	private String sqlQuery;
	private SpartiMetadata metadata;

	public SQLQueryModifier(String sqlQuery, SpartiMetadata metadata) {
		this.sqlQuery = sqlQuery;
		this.metadata = metadata;
	}

	public String convertVPToSparti(HashMap<String, String> newPropTableNames, HashMap<String, ArrayList<String>> queryJoins) {

		String pattern = "\\(SELECT (.*?)\\n\\t FROM (.*?)\\n\\t( WHERE (.*?)\\n)?";
		sqlQuery = sqlQuery.replaceAll("\\n\\t\\s\\n\\t", "\n\t");
		Pattern r = Pattern.compile(pattern, Pattern.DOTALL);
		Matcher m = r.matcher(sqlQuery);

		String newQuery = sqlQuery;
		String newWhere = "";

		while (m.find()) {
			String subquery = m.group();
			String expr = m.group(1);
			String queryProperty = m.group(2).trim();
			String where = m.group(4);

			boolean isSPARTIQuery = false;

			if (newPropTableNames.containsKey(queryProperty)) {
				//Replace property table name with SPARTI table name
				subquery = subquery.replace(queryProperty, newPropTableNames.get(queryProperty));
				isSPARTIQuery = true;
			}

			if (where != null) {
//	            where = where.replace(":","__");
				subquery = subquery.replace(":", "__");
			}


//			if (where != null) {
//				newWhere = where;
//				if (isSPARTIQuery && queryJoins.size() > 0) {
//					newWhere += addSemanticFilter(queryProperty, newPropTableNames, where, queryJoins);
//					subquery = subquery.replace(where, newWhere);
//				}
//			} else {
//				if (isSPARTIQuery && queryJoins.size() > 0)
//					subquery += addSemanticFilter(queryProperty, newPropTableNames, null, queryJoins);
//			}

			newQuery = newQuery.replace(m.group(), subquery);
		}
		return newQuery;
	}


	private String addSemanticFilter(String queryProperty, HashMap<String, String> newPropTableNames, String oldWhere, HashMap<String, ArrayList<String>> queryJoins) {

		HashSet<String> candidate = new HashSet<>();
		//Get all permutations of the join in order to determine the appropriate column names
		for (Map.Entry<String, ArrayList<String>> entry : queryJoins.entrySet()) {
			HashSet<String> freqJoins = new HashSet<>();
			List<String> variableList = entry.getValue();
			List<List<String>> allcombine = new ArrayList<>();
			for (int i = 0; i < variableList.size(); i++) {
				for (int j = i + 1; j < variableList.size(); j++) {
					List<String> combinations = new ArrayList<>();
					combinations.add(variableList.get(i));
					combinations.add(variableList.get(j));
					allcombine.add(combinations);
				}
			}
			if (variableList.size() > 2)
				allcombine.add(variableList);
			for (List candid : allcombine) {
				Permute perm = new Permute();
				List<List<String>> joinColumns = perm.permute(candid, 0);
				for (List<String> combination : joinColumns) {
					if (combination.get(0).startsWith(queryProperty))
						freqJoins.add(StringUtils.join(combination, "_JOIN_"));
				}
			}

			for (String joinName : freqJoins) {
				String[] joinCond = joinName.split("\\_JOIN\\_");
				ArrayList<String> tableJoins = new ArrayList<>();
				for (String p : joinCond) {
					tableJoins.add(p.split("\\_TRP")[0]);
				}
				Map mp = metadata.getSpartiTableInfo(newPropTableNames.get(queryProperty));
				String schema = (String) mp.get("partitionProperties");
				String[] subTables = schema.split(",");
				int matchingEntries = 0;
				for (String jn : tableJoins) {
					if (Arrays.asList(subTables).contains(jn)) {
						matchingEntries += 1;
					}
				}
				if (matchingEntries <= tableJoins.size()) {
					candidate.add(joinName);
				}
			}
		}
		//Determine which candidate will be used to read in the data based on
		//the number of entries it contains (the smaller the better)
		long minCount = Long.MAX_VALUE;
		String minCombination = null;
		for (String cand : candidate) {
			long count = metadata.getSemanticColumnCount(newPropTableNames.get(queryProperty), cand); //TODO maybe get the schema first better ?
			if (count <= 0) //Does not exist
				continue;
			if (count < minCount) {
				minCount = count;
				minCombination = cand;
			}
		}

		String newWhere;
		if (minCombination != null) {
			if (oldWhere != null)
				newWhere = " AND " + minCombination + " IS NOT NULL";
			else
				newWhere = " WHERE " + minCombination + " IS NOT NULL";
		} else {
			newWhere = "";
		}
		return newWhere;
	}

	public String convertVPToBaseTable(HashMap<String, String> newPropTableNames) {

		String pattern = "SELECT (.*?)\\n\\t FROM (.*?)\\n\\t( WHERE (.*?)\\n\\t)?";
		sqlQuery = sqlQuery.replaceAll("\\n\\t\\s\\n\\t", "\n\t");
		Pattern r = Pattern.compile(pattern, Pattern.DOTALL);
		Matcher m = r.matcher(sqlQuery);

		String newQuery = sqlQuery;

		while (m.find()) {
			String subquery = m.group();
			String expr = m.group(1);
			String queryProperty = m.group(2).trim();
			String where = m.group(4);
			boolean isSPARTIQuery = false;

			if (newPropTableNames.containsKey(queryProperty)) {
				//Replace property table name with SPARTI table name
				subquery = subquery.replace(queryProperty, newPropTableNames.get(queryProperty));
			}
			newQuery = newQuery.replace(m.group(), subquery);
		}
		return newQuery;

	}
}
