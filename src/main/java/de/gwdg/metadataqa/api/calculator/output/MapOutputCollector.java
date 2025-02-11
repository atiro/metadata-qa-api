package de.gwdg.metadataqa.api.calculator.output;

import de.gwdg.metadataqa.api.interfaces.Calculator;
import de.gwdg.metadataqa.api.interfaces.MetricResult;
import de.gwdg.metadataqa.api.util.CompressionLevel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapOutputCollector implements OutputCollector {

  Map<String, Object> result = new LinkedHashMap<>();

  @Override
  public void addResult(Calculator calculator, List<MetricResult> metricResults, CompressionLevel compressionLevel) {
    for (MetricResult metricResult : metricResults)
      for (Map.Entry<String, ?> entry : metricResult.getResultMap().entrySet())
        result.put(metricResult.getName() + ":" + entry.getKey(), entry.getValue());
  }

  @Override
  public Object getResults() {
    return result;
  }
}
