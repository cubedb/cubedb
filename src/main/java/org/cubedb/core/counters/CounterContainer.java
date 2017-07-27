package org.cubedb.core.counters;

import org.cubedb.core.lookups.Lookup;
import org.cubedb.core.beans.GroupedSearchResultRow;
import org.cubedb.core.beans.SearchResult;

import java.util.Map;

public class CounterContainer {

  private final Lookup fieldLookup;
  private final Lookup metricLookup;
  private final Map<String, Lookup> lookups;
  private MultiDimensionalCounter sideCounters;

  private boolean isGroupLookup;
  private String groupFieldName;

  public CounterContainer(Lookup fieldLookup, Lookup metricLookup, Map<String, Lookup> lookups) {
    this.fieldLookup = fieldLookup;
    this.metricLookup = metricLookup;
    this.lookups = lookups;
  }

  public void initSideCounters() {
    isGroupLookup = false;
    groupFieldName = SearchResult.FAKE_GROUP_FIELD_NAME;
    int[] fieldSizes = new int[fieldLookup.size()];
    for (int i = 0; i < fieldSizes.length; i++) {
      fieldSizes[i] = lookups.get(fieldLookup.getKey(i)).size();
    }
    // Using the simplest multikey, that doesn't know anything about the
    // group
    sideCounters =
        new ArrayBasedMultiDimensionalCounter(metricLookup.size(), fieldLookup.size(), fieldSizes);
  }

  public void initGroupedSideCounters(String groupFieldName) {
    isGroupLookup = true;
    this.groupFieldName = groupFieldName;
    int[] fieldSizes = new int[fieldLookup.size()];
    for (int i = 0; i < fieldSizes.length; i++) {
      fieldSizes[i] = lookups.get(fieldLookup.getKey(i)).size() + 1;
    }
    // Using a bit more sophisticated multikey, that one knows about
    // grouping
    sideCounters =
        new ListListMultiDimensionalCounter(metricLookup.size(), fieldLookup.size(), fieldSizes);
  }

  public void add(
      int fieldId, int columnValueId, int groupFieldValueId, int metricIndex, long metricValue) {
    // please note that the order of fields is mixed: metric index goes
    // first
    sideCounters.add(metricValue, metricIndex, fieldId, columnValueId, groupFieldValueId);
  }

  public void saveResultTo(Map<GroupedSearchResultRow, Long> groupedResult) {
    Lookup groupFieldLookup = lookups.get(groupFieldName);
    sideCounters.forEach(
        (k0, k1, k2, k3, value) -> {
          final int metricIndex = k0;
          String metricName = metricLookup.getKey(metricIndex);

          final int fieldId = k1;
          String sideName = fieldLookup.getKey(fieldId);
          Lookup sideLookup = lookups.get(sideName);

          final int columnValueId = k2;
          String sideValue = sideLookup.getKey(columnValueId);

          final int groupFieldValueId = k3;
          String groupFieldValue =
              isGroupLookup
              ? groupFieldLookup.getKey(groupFieldValueId)
              : SearchResult.FAKE_GROUP_FIELD_VALUE;

          GroupedSearchResultRow row =
              new GroupedSearchResultRow(
                  groupFieldName, groupFieldValue, sideName, sideValue, metricName);

          groupedResult.put(row, value);
        });
  }
}
