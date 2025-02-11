package de.gwdg.metadataqa.api.calculator;

import de.gwdg.metadataqa.api.interfaces.MetricResult;
import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.model.pathcache.PathCache;
import de.gwdg.metadataqa.api.counter.FieldCounter;
import de.gwdg.metadataqa.api.interfaces.Calculator;
import de.gwdg.metadataqa.api.problemcatalog.FieldCounterBasedResult;
import de.gwdg.metadataqa.api.schema.Schema;
import de.gwdg.metadataqa.api.uniqueness.SolrClient;
import de.gwdg.metadataqa.api.uniqueness.UniquenessExtractor;
import de.gwdg.metadataqa.api.uniqueness.UniquenessField;
import de.gwdg.metadataqa.api.uniqueness.UniquenessFieldCalculator;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class UniquenessCalculator implements Calculator, Serializable {

  public static final String CALCULATOR_NAME = "uniqueness";

  public static final String SUFFIX = "_txt";
  public static final int SUFFIX_LENGTH = SUFFIX.length();

  private UniquenessExtractor extractor;
  private List<UniquenessField> solrFields;

  private final SolrClient solrClient;

  private FieldCounter<Double> resultMap;

  public UniquenessCalculator(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  public UniquenessCalculator(SolrClient solrClient, Schema schema) {
    this(solrClient);
    extractor = new UniquenessExtractor();
    initialize(schema);
  }

  private void initialize(Schema schema) {
    solrFields = new ArrayList<>();
    for (JsonBranch jsonBranch : schema.getIndexFields()) {
      var field = new UniquenessField(jsonBranch.getLabel());
      field.setJsonPath(
        jsonBranch.getAbsoluteJsonPath().replace("[*]", "")
      );
      var solrField = jsonBranch.getIndexField();
      if (solrField.endsWith(SUFFIX)) {
        solrField = solrField.substring(0, solrField.length() - SUFFIX_LENGTH) + "_ss";
      }
      field.setSolrField(solrField);

      var solrResponse = solrClient.getSolrSearchResponse(solrField, "*");
      var numFound = extractor.extractNumFound(solrResponse, "total");
      field.setTotal(numFound);
      field.setScoreForUniqueValue(
        UniquenessFieldCalculator.calculateScore(numFound, 1.0)
      );

      solrFields.add(field);
    }
  }

  @Override
  public String getCalculatorName() {
    return CALCULATOR_NAME;
  }

  @Override
  public List<MetricResult> measure(PathCache cache) {
    String recordId = cache.getRecordId();
    if (StringUtils.isNotBlank(recordId) && recordId.startsWith("/")) {
      recordId = recordId.substring(1);
    }

    resultMap = new FieldCounter<>();
    for (UniquenessField solrField : solrFields) {
      var fieldCalculator = new UniquenessFieldCalculator(
          cache, recordId, solrClient, solrField
      );
      fieldCalculator.calculate();
      resultMap.put(
          solrField.getSolrField() + "/count",
          fieldCalculator.getAverageCount()
      );
      resultMap.put(
          solrField.getSolrField() + "/score",
          fieldCalculator.getAverageScore()
      );
    }
    return List.of(new FieldCounterBasedResult<Double>(getCalculatorName(), resultMap));
  }

  public String getTotals() {
    List<Integer> totals = new ArrayList<>();
    for (UniquenessField field : solrFields) {
      totals.add(field.getTotal());
    }
    return StringUtils.join(totals, ",");
  }

  @Override
  public List<String> getHeader() {
    List<String> headers = new ArrayList<>();
    for (UniquenessField field : solrFields) {
      headers.add(field.getSolrField() + "/count");
      headers.add(field.getSolrField() + "/score");
    }
    return headers;
  }

  public List<UniquenessField> getSolrFields() {
    return solrFields;
  }
}
