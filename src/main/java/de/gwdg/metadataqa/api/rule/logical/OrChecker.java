package de.gwdg.metadataqa.api.rule.logical;

import de.gwdg.metadataqa.api.counter.FieldCounter;
import de.gwdg.metadataqa.api.json.JsonBranch;
import de.gwdg.metadataqa.api.model.pathcache.PathCache;
import de.gwdg.metadataqa.api.rule.RuleChecker;
import de.gwdg.metadataqa.api.rule.RuleCheckerOutput;
import de.gwdg.metadataqa.api.rule.RuleCheckingOutputType;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class OrChecker extends LogicalChecker {

  private static final Logger LOGGER = Logger.getLogger(OrChecker.class.getCanonicalName());

  private static final long serialVersionUID = 1114999259831619599L;
  public static final String PREFIX = "or";

  /**
   * @param field The field
   * @param checkers The list of checkers
   */
  public OrChecker(JsonBranch field, List<RuleChecker> checkers) {
    this(field, field.getLabel(), checkers);
  }

  public OrChecker(JsonBranch field, String header, List<RuleChecker> checkers) {
    super(field, header + ":" + PREFIX + ":" + getChildrenHeader(checkers));
    this.checkers = checkers;
  }

  @Override
  public void update(PathCache cache, FieldCounter<RuleCheckerOutput> results) {
    var allPassed = false;
    var isNA = false;
    FieldCounter<RuleCheckerOutput> localResults = new FieldCounter<>();
    for (RuleChecker checker : checkers) {
      checker.update(cache, localResults);
      if (localResults.get(checker.getHeader()).getType().equals(RuleCheckingOutputType.PASSED)) {
        allPassed = true;
        break;
      }
    }
    results.put(getHeader(), new RuleCheckerOutput(this, isNA, allPassed));
  }
}
