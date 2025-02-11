package de.gwdg.metadataqa.api.calculator.output;

import de.gwdg.metadataqa.api.interfaces.Calculator;
import de.gwdg.metadataqa.api.interfaces.MetricResult;
import de.gwdg.metadataqa.api.util.CompressionLevel;

import java.util.ArrayList;
import java.util.List;

public class ObjectListOutputCollector implements OutputCollector {

  List<Object> result = new ArrayList<>();

  @Override
  public void addResult(Calculator calculator, List<MetricResult> metricResults, CompressionLevel compressionLevel) {
    for (MetricResult metricResult : metricResults)
      result.addAll(metricResult.getCsv());
  }

  @Override
  public Object getResults() {
    return result;
  }
}
