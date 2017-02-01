// 
// Author - Jack Hebert (jhebert@cs.washington.edu) 
// Copyright 2007 
// Distributed under GPLv3 
// 
// Modified - Dino Konstantopoulos
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 
package builder;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.*;
import java.lang.StringBuilder;

/* 
 * This class reads in a serialized download of wikispecies, extracts out the links, and 
 * foreach link: 
 *   emits (currPage, (linkedPage, 1)) 
 * 
 * 
 */
public class SpeciesGraphBuilderMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public ArrayList<String> outlinks = null;

	private static int counter = 0;

	// private static int count = 0;
	public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter) throws IOException {
		// Prepare the input data.
		// System.out.println(value.toString()) ;
		// outlinks = SpeciesGraphBuilder.outlinks;
		outlinks = new ArrayList();
		String page = value.toString();
		
		String textStr = value.toString();
		boolean found = false;
		//System.out.println(textStr);
		//System.out.println("######################################################################");
		Pattern regex = Pattern.compile("[^\\x00-\\x7F]+");
		Matcher match = regex.matcher(textStr);
		//Pattern regex2 = Pattern.compile("\bTaxonavigation\b");
		Pattern regex2 = Pattern.compile("[Tt][Aa][Xx][Oo][Nn][Aa][Vv][Ii][Gg][Aa][Tt][Ii][Oo][Nn]");
		Matcher match2 = regex2.matcher(textStr);
		if (!(match.find()) && match2.find()) {
			{
			//	System.out.println(
				//		"******************************************************************************************************************");
				String title = this.GetTitle(page, reporter);

				Pattern regex1 = Pattern.compile("Category:");
				regex = Pattern.compile("Template:");
				Matcher match1 = regex1.matcher(title);
				match = regex.matcher(title);
				if (!(match.find() || match1.find())) {
					if (title.length() > 0) {
						reporter.setStatus(title);
					} else {
						return;
					}

					regex = Pattern.compile("(== Taxonavigation ==|==Taxonavigation==)([^(==)]+)");
					match = regex.matcher(textStr);
					while (match.find()) {
						page = match.group();
						// System.out.println("=====================================================================================================================================================================================");
						// System.out.println(page);
						// System.out.println("=====================================================================================================================================================================================");
						getLinks(page, outlinks);
						// GetOutlinks(page);
					}

					regex = Pattern.compile("(== Name ==|==Name==)([^(==)]+)");
					match = regex.matcher(textStr);

					while (match.find()) {
						getLinks(page, outlinks);
						found = true;
					}

					// outlinks = this.GetOutlinks(page);
					StringBuilder builder = new StringBuilder();

					for (String link : outlinks) {
						link = link.replace(" ", "_");
						link = link.replace(":", "_");
						// builder.append(" ");
						// builder.append(link);
						builder.append(" ").append(link);
					}

					
					String builderString = builder.toString();
					
					if (counter < 10) {
						System.out.println(builderString);
						counter++;
					}
					output.collect(new Text(title), new Text(builderString));
				}
			}
		}
	}

	public String GetTitle(String page, Reporter reporter) throws IOException {
		/*
		 * int end = page.indexOf(","); if (-1 == end) return ""; return
		 * page.substring(0, end);
		 */

		int end;
		String title = "";
		// ArrayList<String> outlinks = new ArrayList<String>();
		int start = page.indexOf("<title>");
		while (start > 0) {
			start = start + 7;
			end = page.indexOf("</title>", start);
			// if((end==-1)||(end-start<0))
			if (end == -1) {
				break;
			}
			// substring from the value in [[ ]]
			String toAdd = page.substring(start);
			title = toAdd.substring(0, end - start);
			// outlinks.add(toAdd);
			start = page.indexOf("<title>", end + 1);
		}
		if (title.equals(""))
			return "";
		else {
			// System.out.println(title);
			return title;
		}
	}

	public ArrayList<String> GetOutlinks(String page) {
		int end;
		// ArrayList<String> outlinks = new ArrayList<String>();

		int start = page.indexOf("[[");
		while (start > 0) {
			start = start + 2;
			end = page.indexOf("]]", start);
			// if((end==-1)||(end-start<0))
			if (end == -1) {
				break;
			}
			// substring from the value in [[ ]]
			String toAdd = page.substring(start);
			toAdd = toAdd.substring(0, end - start);
			outlinks.add(toAdd);
			start = page.indexOf("[[", end + 1);
		}
		return outlinks;
	}

	private void getLinks(String page, List<String> links) {
		int start = page.indexOf("[[");
		int end;
		while (start > 0) {
			start = start + 2;
			end = page.indexOf("]]", start);
			if (end == -1) {
				break;
			}
			String toAdd = page.substring(start);
			toAdd = toAdd.substring(0, (end - start));
			links.add(toAdd);
			start = page.indexOf("[[", end + 1);
		}
		return;
	}
}
