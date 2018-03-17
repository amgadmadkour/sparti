package edu.purdue.sparti.data.parsers.datasets;

import edu.purdue.sparti.model.rdf.RDFDataset;
import edu.purdue.sparti.model.rdf.NameSpaceHandler;

import java.io.*;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Amgad Madkour
 */
public class Convert2Prefix {

	public static void main(String[] args) {

		String dataset = args[0];
		String subject, property, object;

		//Triple regexp
		Pattern p = Pattern.compile("(.+?)\\s(.+?)\\s(.+?)\\s\\.");
		Matcher m;
		//Read input from the input stream
		//BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		Scanner in = new Scanner(System.in);
		//Namespace handling for reducing the URI
		NameSpaceHandler nsHandler = null;

		//Check which dataset the namespace will run over
		if (dataset.equals("watdiv"))
			nsHandler = new NameSpaceHandler(RDFDataset.WATDIV);
		else if (dataset.equals("lubm"))
			nsHandler = new NameSpaceHandler(RDFDataset.LUBM);
		else if (dataset.equals("yago"))
			nsHandler = new NameSpaceHandler(RDFDataset.YAGO);
		else
			System.exit(0);

//		try {
			String s;
		while (in.hasNextLine()) {
			s = in.nextLine();
			if (s.endsWith(".")) {
				m = p.matcher(s);
				if (m.matches()) {
					subject = nsHandler.parse(m.group(1));
					property = nsHandler.parse(m.group(2));
					object = nsHandler.parse(m.group(3));
					//Print to STDOUT
					System.out.println(subject + " " + property + " " + object + " .");
				}
			}
			if (s.length() == 0 || s == null)
				break;
			}
		System.exit(0);
//		} catch (IOException exp) {
//			exp.printStackTrace();
//		}
	}
}
