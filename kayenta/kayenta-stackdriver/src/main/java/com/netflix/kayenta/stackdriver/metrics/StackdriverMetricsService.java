/*
 * Copyright 2017 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.kayenta.stackdriver.metrics;

import com.google.api.Metric;
import com.google.api.MetricDescriptor;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.ListMetricDescriptorsRequest;
import com.google.monitoring.v3.ListMetricDescriptorsResponse;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.ListTimeSeriesResponse;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.netflix.kayenta.canary.CanaryConfig;
import com.netflix.kayenta.canary.CanaryMetricConfig;
import com.netflix.kayenta.canary.CanaryScope;
import com.netflix.kayenta.canary.providers.metrics.QueryConfigUtils;
import com.netflix.kayenta.canary.providers.metrics.StackdriverCanaryMetricSetQueryConfig;
import com.netflix.kayenta.google.security.GoogleNamedAccountCredentials;
import com.netflix.kayenta.metrics.MetricSet;
import com.netflix.kayenta.metrics.MetricsService;
import com.netflix.kayenta.security.AccountCredentials;
import com.netflix.kayenta.security.AccountCredentialsRepository;
import com.netflix.kayenta.stackdriver.canary.StackdriverCanaryScope;
import com.netflix.kayenta.stackdriver.config.StackdriverConfigurationProperties;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

@Builder
@Slf4j
public class StackdriverMetricsService implements MetricsService {

  @NotNull @Singular @Getter private List<String> accountNames;

  @Autowired private final AccountCredentialsRepository accountCredentialsRepository;

  @Autowired private final Registry registry;

  @Autowired private final StackdriverConfigurationProperties stackdriverConfigurationProperties;

  @Builder.Default private List<MetricDescriptor> metricDescriptorsCache = Collections.emptyList();

  @Override
  public String getType() {
    return StackdriverCanaryMetricSetQueryConfig.SERVICE_TYPE;
  }

  @Override
  public boolean servicesAccount(String accountName) {
    return accountNames.contains(accountName);
  }

  @Override
  public String buildQuery(
      String metricsAccountName,
      CanaryConfig canaryConfig,
      CanaryMetricConfig canaryMetricConfig,
      CanaryScope canaryScope) {
    StackdriverCanaryMetricSetQueryConfig queryConfig =
        (StackdriverCanaryMetricSetQueryConfig) canaryMetricConfig.getQuery();
    StackdriverCanaryScope stackdriverCanaryScope = (StackdriverCanaryScope) canaryScope;
    String projectId = determineProjectId(metricsAccountName, stackdriverCanaryScope);
    String location = stackdriverCanaryScope.getLocation();
    String scope = stackdriverCanaryScope.getScope();
    String resourceType =
        StringUtils.hasText(queryConfig.getResourceType())
            ? queryConfig.getResourceType()
            : stackdriverCanaryScope.getResourceType();

    String customFilter =
        QueryConfigUtils.expandCustomFilter(
            canaryConfig,
            queryConfig,
            stackdriverCanaryScope,
            new String[] {"project", "resourceType", "scope", "location"});
    String filter =
        "metric.type=\""
            + queryConfig.getMetricType()
            + "\""
            + " AND resource.type="
            + resourceType;

    // TODO(duftler): Replace direct string-manipulating with helper functions.
    // TODO-maybe(duftler): Replace this logic with a library of templates, one for each resource
    // type.
    if (StringUtils.isEmpty(customFilter)) {
      if ("gce_instance".equals(resourceType)) {
        if (StringUtils.isEmpty(location)) {
          throw new IllegalArgumentException(
              "Location (i.e. region) is required when resourceType is 'gce_instance'.");
        }

        if (StringUtils.isEmpty(scope)) {
          throw new IllegalArgumentException(
              "Scope is required when resourceType is 'gce_instance'.");
        }

        filter +=
            " AND project="
                + projectId
                + " AND metadata.user_labels.\"spinnaker-region\"="
                + location
                + " AND metadata.user_labels.\"spinnaker-server-group\"="
                + scope;
      } else if ("aws_ec2_instance".equals(resourceType)) {
        if (StringUtils.isEmpty(location)) {
          throw new IllegalArgumentException(
              "Location (i.e. region) is required when resourceType is 'aws_ec2_instance'.");
        }

        if (StringUtils.isEmpty(scope)) {
          throw new IllegalArgumentException(
              "Scope is required when resourceType is 'aws_ec2_instance'.");
        }

        filter +=
            " AND resource.labels.region=\"aws:"
                + location
                + "\""
                + " AND metadata.user_labels.\"aws:autoscaling:groupname\"="
                + scope;
      } else if ("gae_app".equals(resourceType)) {
        if (StringUtils.isEmpty(scope)) {
          throw new IllegalArgumentException("Scope is required when resourceType is 'gae_app'.");
        }

        filter += " AND project=" + projectId + " AND resource.labels.version_id=" + scope;

        Map<String, String> extendedScopeParams = stackdriverCanaryScope.getExtendedScopeParams();

        if (extendedScopeParams != null && extendedScopeParams.containsKey("service")) {
          filter += " AND resource.labels.module_id=" + extendedScopeParams.get("service");
        }
      } else if (Arrays.asList("k8s_container", "k8s_pod", "k8s_node").contains(resourceType)) {
        // TODO(duftler): Figure out where it makes sense to use 'scope'. It is available as a
        // template variable binding,
        // and maps to the control and experiment scopes. Will probably be useful to use expressions
        // in those fields in
        // the ui, and then map 'scope' to some user label value in a custom filter template.
        // TODO(duftler): Should cluster_name be automatically included or required?
        filter += " AND project=" + projectId;
        Map<String, String> extendedScopeParams = stackdriverCanaryScope.getExtendedScopeParams();

        if (extendedScopeParams != null) {
          List<String> resourceLabelKeys =
              Arrays.asList(
                  "location",
                  "node_name",
                  "cluster_name",
                  "pod_name",
                  "container_name",
                  "namespace_name");

          for (String resourceLabelKey : resourceLabelKeys) {
            if (extendedScopeParams.containsKey(resourceLabelKey)) {
              filter +=
                  " AND resource.labels."
                      + resourceLabelKey
                      + "="
                      + extendedScopeParams.get(resourceLabelKey);
            }
          }

          for (String extendedScopeParamsKey : extendedScopeParams.keySet()) {
            if (extendedScopeParamsKey.startsWith("user_labels.")) {
              String userLabelKey = extendedScopeParamsKey.substring(12);

              filter +=
                  " AND metadata.user_labels.\""
                      + userLabelKey
                      + "\"=\""
                      + extendedScopeParams.get(extendedScopeParamsKey)
                      + "\"";
            }
          }
        }
      } else if ("gke_container".equals(resourceType)) {
        filter += " AND project=" + projectId;
        Map<String, String> extendedScopeParams = stackdriverCanaryScope.getExtendedScopeParams();

        if (extendedScopeParams != null) {
          List<String> resourceLabelKeys =
              Arrays.asList(
                  "cluster_name",
                  "namespace_id",
                  "instance_id",
                  "pod_id",
                  "container_name",
                  "zone");

          for (String resourceLabelKey : resourceLabelKeys) {
            if (extendedScopeParams.containsKey(resourceLabelKey)) {
              filter +=
                  " AND resource.labels."
                      + resourceLabelKey
                      + "="
                      + extendedScopeParams.get(resourceLabelKey);
            }
          }

          for (String extendedScopeParamsKey : extendedScopeParams.keySet()) {
            if (extendedScopeParamsKey.startsWith("user_labels.")) {
              String userLabelKey = extendedScopeParamsKey.substring(12);

              filter +=
                  " AND metadata.user_labels.\""
                      + userLabelKey
                      + "\"=\""
                      + extendedScopeParams.get(extendedScopeParamsKey)
                      + "\"";
            }
          }
        }
      } else if (!"global".equals(resourceType)) {
        throw new IllegalArgumentException(
            "Resource type '"
                + resourceType
                + "' not yet explicitly supported. If you employ a "
                + "custom filter, you may use any resource type you like.");
      }
    } else {
      filter += " AND " + customFilter;
    }

    log.debug("filter={}", filter);

    return filter;
  }

  private <E extends Enum<E>> E trinary(
      String first, String second, E defaultToUse, Class<E> classToUse) {
    if (ObjectUtils.isNotEmpty(first)) return Enum.valueOf(classToUse, first);
    if (ObjectUtils.isNotEmpty(second)) return Enum.valueOf(classToUse, second);
    return defaultToUse;
  }

  @Override
  public List<MetricSet> queryMetrics(
      String metricsAccountName,
      CanaryConfig canaryConfig,
      CanaryMetricConfig canaryMetricConfig,
      CanaryScope canaryScope)
      throws IOException {
    if (!(canaryScope instanceof StackdriverCanaryScope)) {
      throw new IllegalArgumentException(
          "Canary scope not instance of StackdriverCanaryScope: "
              + canaryScope
              + ". One common cause is having multiple METRICS_STORE accounts configured but "
              + "neglecting to explicitly specify which account to use for a given request.");
    }

    StackdriverCanaryScope stackdriverCanaryScope = (StackdriverCanaryScope) canaryScope;
    GoogleNamedAccountCredentials stackdriverCredentials =
        accountCredentialsRepository.getRequiredOne(metricsAccountName);
    MetricServiceClient monitoring = stackdriverCredentials.getMonitoring();
    StackdriverCanaryMetricSetQueryConfig stackdriverMetricSetQuery =
        (StackdriverCanaryMetricSetQueryConfig) canaryMetricConfig.getQuery();
    String projectId = determineProjectId(metricsAccountName, stackdriverCanaryScope);
    String location = stackdriverCanaryScope.getLocation();
    String resourceType =
        StringUtils.hasText(stackdriverMetricSetQuery.getResourceType())
            ? stackdriverMetricSetQuery.getResourceType()
            : stackdriverCanaryScope.getResourceType();
    Aggregation.Reducer crossSeriesReducer =
        trinary(
            stackdriverMetricSetQuery.getCrossSeriesReducer(),
            stackdriverCanaryScope.getCrossSeriesReducer(),
            Aggregation.Reducer.REDUCE_MEAN,
            Aggregation.Reducer.class);

    Aggregation.Aligner perSeriesAligner =
        trinary(
            stackdriverMetricSetQuery.getPerSeriesAligner(),
            stackdriverMetricSetQuery.getPerSeriesAligner(),
            Aggregation.Aligner.ALIGN_MEAN,
            Aggregation.Aligner.class);

    if (StringUtils.isEmpty(projectId)) {
      projectId = stackdriverCredentials.getProject();
    }

    if (StringUtils.isEmpty(resourceType)) {
      throw new IllegalArgumentException("Resource type is required.");
    }

    if (StringUtils.isEmpty(stackdriverCanaryScope.getStart())) {
      throw new IllegalArgumentException("Start time is required.");
    }

    if (StringUtils.isEmpty(stackdriverCanaryScope.getEnd())) {
      throw new IllegalArgumentException("End time is required.");
    }

    String filter = buildQuery(metricsAccountName, canaryConfig, canaryMetricConfig, canaryScope);

    long alignmentPeriodSec = stackdriverCanaryScope.getStep();
    Aggregation.Builder aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(alignmentPeriodSec).build())
            .setCrossSeriesReducer(crossSeriesReducer)
            .setPerSeriesAligner(perSeriesAligner);

    //    Monitoring.Projects.TimeSeries.List list =
    //        monitoring
    //            .projects()
    //            .timeSeries()
    //            .list("projects/" + stackdriverCredentials.getProject())
    //            .setIntervalStartTime(stackdriverCanaryScope.getStart().toString())
    //            .setIntervalEndTime(stackdriverCanaryScope.getEnd().toString());

    List<String> groupByFields = stackdriverMetricSetQuery.getGroupByFields();
    if (groupByFields != null) {
      aggregation.addAllGroupByFields(groupByFields);
    }
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setFilter(filter)
            .setName("projects/" + projectId)
            .setInterval(
                TimeInterval.newBuilder()
                    .setStartTime(
                        Timestamp.newBuilder()
                            .setSeconds(stackdriverCanaryScope.getStart().getEpochSecond())
                            .build())
                    .setEndTime(
                        Timestamp.newBuilder()
                            .setSeconds(stackdriverCanaryScope.getEnd().getEpochSecond())
                            .build()))
            .setAggregation(aggregation)
            .build();

    long startTime = registry.clock().monotonicTime();
    ListTimeSeriesResponse response;

    try {
      response = monitoring.listTimeSeriesCallable().call(request);
    } finally {
      long endTime = registry.clock().monotonicTime();
      Id stackdriverFetchTimerId =
          registry.createId("stackdriver.fetchTime").withTag("project", projectId);

      if (!StringUtils.isEmpty(location)) {
        stackdriverFetchTimerId = stackdriverFetchTimerId.withTag("location", location);
      }

      registry.timer(stackdriverFetchTimerId).record(endTime - startTime, TimeUnit.NANOSECONDS);
    }

    long startAsLong = stackdriverCanaryScope.getStart().toEpochMilli();
    long endAsLong = stackdriverCanaryScope.getEnd().toEpochMilli();
    long elapsedSeconds = (endAsLong - startAsLong) / 1000;
    long numIntervals = elapsedSeconds / alignmentPeriodSec;
    long remainder = elapsedSeconds % alignmentPeriodSec;

    if (remainder > 0) {
      numIntervals++;
    }

    List<TimeSeries> timeSeriesList = response.getTimeSeriesList();

    if (timeSeriesList.isEmpty()) {
      // Add placeholder metric set.
      timeSeriesList =
          Collections.singletonList(
              TimeSeries.newBuilder()
                  .setMetric(Metric.newBuilder())
                  .addAllPoints(new ArrayList<>())
                  .build());
    }

    List<MetricSet> metricSetList = new ArrayList<>();

    for (TimeSeries timeSeries : timeSeriesList) {
      List<Point> points = timeSeries.getPointsList();

      if (points.size() != numIntervals) {
        String pointOrPoints = numIntervals == 1 ? "point" : "points";

        log.warn(
            "Expected {} data {}, but received {}.", numIntervals, pointOrPoints, points.size());
      }

      Collections.reverse(points);

      Instant responseStartTimeInstant =
          !points.isEmpty()
              ? Instant.ofEpochSecond(points.get(0).getInterval().getStartTime().getSeconds())
              : stackdriverCanaryScope.getStart();
      long responseStartTimeMillis = responseStartTimeInstant.toEpochMilli();

      Instant responseEndTimeInstant =
          !points.isEmpty()
              ? Instant.ofEpochSecond(
                  points.get(points.size() - 1).getInterval().getEndTime().getSeconds())
              : stackdriverCanaryScope.getEnd();

      // TODO(duftler): What if there are no data points?
      List<Double> pointValues;

      if (points.isEmpty()) {
        log.warn("No data points available.");
        pointValues = Collections.emptyList();
      } else {
        switch (timeSeries.getValueType()) {
          case INT64:
            pointValues =
                points.stream().map(point -> (double) point.getValue().getInt64Value()).toList();
            break;
          case DOUBLE:
            pointValues = points.stream().map(point -> point.getValue().getDoubleValue()).toList();
            break;
          case STRING:
            log.warn(
                "Expected timeSeries value type to be either DOUBLE or INT64. Got {}.",
                timeSeries.getValueType());
            pointValues = points.stream().map(point -> point.getValue().getDoubleValue()).toList();
            break;
          default:
            log.warn("Time series type was not one we can handle");
            pointValues = Collections.emptyList();
        }
      }

      MetricSet.MetricSetBuilder metricSetBuilder =
          MetricSet.builder()
              .name(canaryMetricConfig.getName())
              .startTimeMillis(responseStartTimeMillis)
              .startTimeIso(responseStartTimeInstant.toString())
              .endTimeMillis(responseEndTimeInstant.toEpochMilli())
              .endTimeIso(responseEndTimeInstant.toString())
              .stepMillis(alignmentPeriodSec * 1000)
              .values(pointValues);

      Map<String, String> filteredLabels = new HashMap<>();

      MonitoredResource monitoredResource = timeSeries.getResource();
      if (monitoredResource != null) {
        Map<String, String> labels = monitoredResource.getLabels();

        if (labels != null) {
          filteredLabels.putAll(
              labels.entrySet().stream()
                  .filter(entry -> entry.getKey() != "project_id")
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
      }

      Metric metric = timeSeries.getMetric();
      if (metric != null) {
        Map<String, String> labels = metric.getLabels();

        if (labels != null) {
          filteredLabels.putAll(labels);
        }
      }

      metricSetBuilder.tags(filteredLabels);

      metricSetBuilder.attribute("query", filter);
      metricSetBuilder.attribute("crossSeriesReducer", crossSeriesReducer.toString());
      metricSetBuilder.attribute("perSeriesAligner", perSeriesAligner.toString());

      metricSetList.add(metricSetBuilder.build());
    }

    return metricSetList;
  }

  private String determineProjectId(
      String metricsAccountName, StackdriverCanaryScope stackdriverCanaryScope) {
    String projectId = stackdriverCanaryScope.getProject();

    if (StringUtils.isEmpty(projectId)) {
      GoogleNamedAccountCredentials stackdriverCredentials =
          accountCredentialsRepository.getRequiredOne(metricsAccountName);

      projectId = stackdriverCredentials.getProject();
    }

    return projectId;
  }

  @Override
  public List<Map<String, ?>> getMetadata(String metricsAccountName, String filter) {
    List<Map<String, ?>> result = new LinkedList<>();
    if (!StringUtils.isEmpty(filter)) {
      String lowerCaseFilter = filter.toLowerCase();

      metricDescriptorsCache.stream()
          .filter(
              metricDescriptor ->
                  metricDescriptor.getName().toLowerCase().contains(lowerCaseFilter))
          .forEach(
              each -> {
                try {
                  result.add(PropertyUtils.describe(each));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    } else {
      metricDescriptorsCache.stream()
          .forEach(
              each -> {
                try {
                  result.add(PropertyUtils.describe(each));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }
    return result;
  }

  @Scheduled(fixedDelayString = "#{@stackdriverConfigurationProperties.metadataCachingIntervalMS}")
  public void updateMetricDescriptorsCache() throws IOException {
    Set<AccountCredentials> accountCredentialsSet =
        accountCredentialsRepository.getAllOf(AccountCredentials.Type.METRICS_STORE);

    for (AccountCredentials credentials : accountCredentialsSet) {
      if (credentials instanceof GoogleNamedAccountCredentials) {
        GoogleNamedAccountCredentials stackdriverCredentials =
            (GoogleNamedAccountCredentials) credentials;
        log.info("Getting descriptors for {}", stackdriverCredentials.getName());
        ListMetricDescriptorsRequest request =
            ListMetricDescriptorsRequest.newBuilder()
                .setName("projects/" + stackdriverCredentials.getProject())
                .build();
        ListMetricDescriptorsResponse listMetricDescriptorsResponse =
            stackdriverCredentials.getMonitoring().listMetricDescriptorsCallable().call(request);
        List<MetricDescriptor> metricDescriptors =
            listMetricDescriptorsResponse.getMetricDescriptorsList();

        if (!CollectionUtils.isEmpty(metricDescriptors)) {
          // TODO(duftler): Should we instead be building the union across all accounts? This
          // doesn't seem quite right yet.
          metricDescriptorsCache = metricDescriptors;

          log.debug(
              "Updated cache with {} metric descriptors via account {}.",
              metricDescriptors.size(),
              stackdriverCredentials.getName());
        } else {
          log.debug(
              "While updating cache, found no metric descriptors via account {}.",
              stackdriverCredentials.getName());
        }
      }
    }
  }
}
