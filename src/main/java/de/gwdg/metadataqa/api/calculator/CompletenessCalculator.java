package de.gwdg.metadataqa.api.calculator;

import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.json.FieldGroup;
import de.gwdg.metadataqa.api.counter.Counters;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import de.gwdg.metadataqa.api.model.JsonPathCache;
import com.jayway.jsonpath.InvalidJsonException;
import de.gwdg.metadataqa.api.model.XmlFieldInstance;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import de.gwdg.metadataqa.api.schema.Schema;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 * @param <T>
 */
public class CompletenessCalculator<T extends XmlFieldInstance> implements Calculator, Serializable {

	private static final Logger LOGGER = Logger.getLogger(CompletenessCalculator.class.getCanonicalName());

	private String inputFileName;

	// private Counters counters;
	private List<String> missingFields;
	private List<String> emptyFields;
	private List<String> existingFields;
	private Schema schema;

	private boolean collectFields = false;

	private static final List<FieldGroup> FIELD_GROUPS = new ArrayList<>();

	static {
	}

	public CompletenessCalculator() {
		// this.recordID = null;
	}

	public CompletenessCalculator(String recordID) {
		// this.recordID = recordID;
	}

	public CompletenessCalculator(Schema schema) {
		this.schema = schema;
	}

	@Override
	public void measure(JsonPathCache cache, Counters counters) throws InvalidJsonException {
		// Object document = JSON_PROVIDER.parse(jsonString);
		if (collectFields) {
			missingFields = new ArrayList<>();
			emptyFields = new ArrayList<>();
			existingFields = new ArrayList<>();
		}

		for (JsonBranch jsonBranch : schema.getPaths()) {
			evaluateJsonBranch(jsonBranch, cache, counters);
		}

		for (FieldGroup fieldGroup : schema.getFieldGroups()) {
			boolean existing = false;
			for (String field : fieldGroup.getFields()) {
				if (counters.getExistenceMap().get(field) == true) {
					existing = true;
					break;
				}
			}
			counters.increaseInstance(fieldGroup.getCategory(), existing);
		}
	}

	public void evaluateJsonBranch(JsonBranch jsonBranch, JsonPathCache cache, Counters counters) {
		List<T> values = cache.get(jsonBranch.getJsonPath());
		counters.increaseTotal(jsonBranch.getCategories());
		if (values != null && !values.isEmpty()) {
			counters.increaseInstance(jsonBranch.getCategories());
			counters.addExistence(jsonBranch.getLabel(), true);
			counters.addInstance(jsonBranch.getLabel(), values.size());
			if (collectFields) {
				existingFields.add(jsonBranch.getLabel());
			}
		} else {
			counters.addExistence(jsonBranch.getLabel(), false);
			counters.addInstance(jsonBranch.getLabel(), 0);
			if (collectFields) {
				missingFields.add(jsonBranch.getLabel());
			}
		}
	}

	public void collectFields(boolean collectFields) {
		this.collectFields = collectFields;
	}

	public List<String> getMissingFields() {
		return missingFields;
	}

	public List<String> getEmptyFields() {
		return emptyFields;
	}

	public List<String> getExistingFields() {
		return existingFields;
	}

	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}

	public String getInputFileName() {
		return inputFileName;
	}

}
