package de.gwdg.metadataqa.api.rule.pairchecker;

import de.gwdg.metadataqa.api.counter.FieldCounter;
import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import de.gwdg.metadataqa.api.model.pathcache.PathCache;
import de.gwdg.metadataqa.api.rule.RuleCheckerOutput;
import de.gwdg.metadataqa.api.rule.RuleCheckingOutputType;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.LinkedHashMap;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.JSONArray;
import java.nio.file.Paths;
import java.io.IOException;
import java.lang.InterruptedException;
import java.io.FileReader;
import com.opencsv.CSVReader;
//import com.opencsv.exceptions.CsvValidationException;

public class FactChecker extends PropertyPairChecker {

  private static final long serialVersionUID = -5363342097255677979L;
  public static final String PREFIX = "entityabsence";
  protected String fixedValue;
  private HttpClient client;
  private static Map<String, List<Integer>> kbFacts = null;

  public FactChecker(JsonBranch field1, JsonBranch field2) {
    super(field1, field2, PREFIX);

	if(kbFacts == null) {
	  // 
      kbFacts = new LinkedHashMap<>();
      try (CSVReader csvReader = new CSVReader(new FileReader("/tmp/techniques-kb.csv"))) {
        Integer[] years = null;
        String[] values = null;
		try {
          while ((values = csvReader.readNext()) != null) {
			// Earliest Year
			int earliest_year;
			int latest_year = 9999;

            earliest_year = Integer.parseInt(values[1]);

			// Latest Year (if known)
			if(values.length > 2) {
              latest_year = Integer.parseInt(values[2]);
			}

            kbFacts.put(values[0].toLowerCase(), Arrays.asList(earliest_year, latest_year));
		  }
		  // TODO - this throws an unknown class exception at runtime
//	    } catch(CsvValidationException e) {
	    } catch(Exception e) {
	      System.out.println("CSV Validation Exception error");
	    }
	  } catch(IOException e) {
	    System.out.println("KB IO error");
	  }
    } 

  }

  @Override
  public void update(PathCache cache, FieldCounter<RuleCheckerOutput> results) {
    var allPassed = true;
    var isNA = true;
	int unknown_entities = 0;

	// Q why remove array ?
    //List<XmlFieldInstance> sources = cache.get(field2.getAbsoluteJsonPath().replace("[*]", ""));
    // List<XmlFieldInstance> known = cache.get(field1.getAbsoluteJsonPath().replace("[*]", ""));
    List<XmlFieldInstance> materials = cache.get(field1.getAbsoluteJsonPath());
    List<XmlFieldInstance> dates = cache.get(field2.getAbsoluteJsonPath());

	// First we send field1 to a NER service to extract nouns (root)
	if (dates != null && !dates.isEmpty()) {
		Integer earliest_prod = 9999; 
		Integer latest_prod = -9999;

		// Save the earliest and latest dates
		
		for (XmlFieldInstance date : dates) {
		  if (date.hasValue() && date.getValue() != "") {
			  // To avoid date manipulation, we just compare years as integers
			  String date_str = date.getValue();
			  Integer earliest_year;
//			  Integer latest_year = Integer.parseInt(date.getValue().split("-")[0]);
			  if(date_str.charAt(0) == '-') {
			    earliest_year = 0 - Integer.parseInt(date_str.split("-")[1]);
//			    latest_year = 0 - Integer.parseInt(date.getValue().split("-")[0]);
			  } else {
			    earliest_year = Integer.parseInt(date_str.split("-")[0]);
//			    latest_year = Integer.parseInt(date.getValue().split("-")[0]);
			  }

			if(earliest_year < earliest_prod) {
				earliest_prod = earliest_year;
			} 
//			if(latest_year > latest_prod) {
//				latest_prod = latest_year;
//			}
		  }
		}

		if(materials != null) {
	  	  for (XmlFieldInstance material : materials) {
		    if (material.hasValue()) {
			  isNA = false;

			// Lookup material in facts KB to get valid from and to years
			  if(kbFacts.containsKey(material.getValue().toLowerCase())) {
			    System.out.println("Material in KB: " + material.getValue());
				List<Integer>valid_dates = kbFacts.get(material.getValue().toLowerCase());

			    System.out.println("Material KB Earliest " + valid_dates.get(0));
			    System.out.println("Object Earliest " + earliest_prod);

				if(valid_dates.get(0) > earliest_prod) {
			      System.out.println("INVALID DATE");
					allPassed = false;
				} else if(valid_dates.get(1) < latest_prod) {
					allPassed = false;
				}
			  }

			// Compare years. Valid from should be before earliest, valid to should be after latest
			// if latest is set
			// if not set allPassed to false
			
            }
		  }
		}
	}

    results.put(getHeader(), new RuleCheckerOutput(this, isNA, allPassed));
  }

}
