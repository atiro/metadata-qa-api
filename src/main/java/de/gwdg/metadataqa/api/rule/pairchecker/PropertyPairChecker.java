package de.gwdg.metadataqa.api.rule.pairchecker;

import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.rule.BaseRuleChecker;

public abstract class PropertyPairChecker extends BaseRuleChecker {

  private static final long serialVersionUID = -6579708841667005135L;
  protected JsonBranch field1;
  protected JsonBranch field2;

  protected PropertyPairChecker(JsonBranch field1, JsonBranch field2, String prefix) {
    if (field1 == null)
      throw new IllegalArgumentException("field1 should not be null");
    if (field2 == null)
      throw new IllegalArgumentException("field2 should not be null");

    this.field1 = field1;
    this.field2 = field2;
    this.header = String.format("%s:%s:%s", field1.getLabel(), prefix, field2.getLabel());
  }
}
