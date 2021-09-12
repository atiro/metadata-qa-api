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

public class EntityAbsenceChecker extends PropertyPairChecker {

  private static final long serialVersionUID = -5363342097255677979L;
  public static final String PREFIX = "entityabsence";
  protected String fixedValue;
  private HttpClient client;
  private static List<String> kbWords = null;
  private static Map<String, List<String>> NERCache = null;

  public EntityAbsenceChecker(JsonBranch field1, JsonBranch field2) {
    super(field1, field2, PREFIX);

	if(kbWords == null) {
	  // 
	  kbWords = new ArrayList<>();
      try (CSVReader csvReader = new CSVReader(new FileReader("/tmp/places.csv"))) {
        String[] values = null;
		try {
          while ((values = csvReader.readNext()) != null) {
            kbWords.add(values[0].toLowerCase());
		  }
	    } catch(Exception e) {
	      System.out.println("CSV Validation Exception error");
	    }
	  } catch(IOException e) {
	    System.out.println("KB IO error");
	  }
    } 

	if(NERCache == null) {
		NERCache = new LinkedHashMap<>();
	}

  }

  @Override
  public void update(PathCache cache, FieldCounter<RuleCheckerOutput> results) {
    var allPassed = true;
    var isNA = true;
	List <String> new_entities = new ArrayList<>();
	int unknown_entities = 0;

    List<XmlFieldInstance> sources = cache.get(field2.getAbsoluteJsonPath());
    List<XmlFieldInstance> knowns = cache.get(field1.getAbsoluteJsonPath());

	// First we send field1 to a NER service to extract nouns (root)
	if (sources != null && !sources.isEmpty()) {
		List <String> known_entities = new ArrayList<>();

		// We find all the known entities
		if(knowns != null) {
	  	  for (XmlFieldInstance known: knowns) {
			known_entities.add(known.getValue().toLowerCase());
		  }
	    }

		for (XmlFieldInstance source : sources) {
		  if (source.hasValue()) {
	        List<String> ner_words = new ArrayList<>();
			isNA = false;

			// Check if we already have the results and use instead
			if(NERCache != null && NERCache.containsKey(source.getValue())) {
				for (Object word: NERCache.get(source.getValue())) {
					ner_words.add((String)word);
				}
		    } else {

			// We need to send to NER and cache the response
			  var payload_json = new JSONObject().appendField("text", 
					source.getValue()).appendField("model", "en").toJSONString();

			// Query via HTTP API to Spacy
			  var request = HttpRequest.newBuilder(
			      URI.create("http://127.0.0.1:8280/dep"))
                 .header("accept", "application/json")
				 .POST(BodyPublishers.ofString(payload_json))
                 .build();

			  client = HttpClient.newHttpClient();
			
			  HttpResponse<String> ner_response = null;

			  try {
	  		    ner_response = client.send(request, BodyHandlers.ofString()); 
			  } catch(IOException e) {
				System.out.println("IO error");
			  } catch(InterruptedException e) {
				System.out.println("NER request interrupted");
			  }

			  if(ner_response != null) {
	  		    JSONObject ner_json = (JSONObject) JSONValue.parse(ner_response.body());

			    JSONArray words = (JSONArray) ner_json.get("words");
			    for (Object word: words) {
				  JSONObject word_json = (JSONObject) word;
				  // For the moment, only consider tagged nouns
				  if("NN".equals((String)word_json.get("tag"))) { 
					ner_words.add(((String)word_json.get("text")).toLowerCase());
				  }
			    }

				// Save to cache
				NERCache.put(source.getValue(), ner_words);
			  }

			  if(ner_words.size() > 0) {
				for(String word: ner_words) {
					if(kbWords.contains(word)) {
						// Check if we already have it in the target field
					  if(!known_entities.contains(word)) {
						unknown_entities += 1;
						new_entities.add(word);
					  }
					}
				}
			  }
			}

          }
        }
	}

	if(unknown_entities > 0) {
      results.put(getHeader(), new RuleCheckerOutput(RuleCheckingOutputType.FAILED, unknown_entities));
	  // Todo - how to pass to output the details of the failed rule
	} else {
      results.put(getHeader(), new RuleCheckerOutput(RuleCheckingOutputType.PASSED, 0));
	}
  }

}
