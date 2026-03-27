package com.netflix.kayenta.stackdriver.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.MetricDescriptor;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.ListTimeSeriesResponse;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.Timestamp;
import com.netflix.kayenta.canary.CanaryConfig;
import com.netflix.kayenta.canary.CanaryMetricConfig;
import com.netflix.kayenta.canary.providers.metrics.StackdriverCanaryMetricSetQueryConfig;
import com.netflix.kayenta.google.security.GoogleNamedAccountCredentials;
import com.netflix.kayenta.metrics.MetricSet;
import com.netflix.kayenta.security.AccountCredentials;
import com.netflix.kayenta.security.AccountCredentialsRepository;
import com.netflix.kayenta.stackdriver.canary.StackdriverCanaryScope;
import com.netflix.spectator.api.DefaultRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.beanutils.PropertyUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StackdriverMetricsServiceTest {

  private static final String ACCOUNT = "test-account";

  private AccountCredentialsRepository accountCredentialsRepoMock;

  private StackdriverMetricsService stackdriverMetricsService;

  @BeforeEach
  void setup() {
    StackdriverMetricsService.StackdriverMetricsServiceBuilder stackdriverMetricsServiceBuilder =
        StackdriverMetricsService.builder();
    accountCredentialsRepoMock = mock(AccountCredentialsRepository.class);
    stackdriverMetricsServiceBuilder
        .accountCredentialsRepository(accountCredentialsRepoMock)
        .registry(new DefaultRegistry());
    stackdriverMetricsService = stackdriverMetricsServiceBuilder.build();
  }

  @Test
  void readsInt64Metrics() throws IOException {
    GoogleNamedAccountCredentials stackdriverCredentialsMock = new GoogleNamedAccountCredentials();
    stackdriverCredentialsMock.setMonitoring(
        mock(MetricServiceClient.class, Mockito.RETURNS_DEEP_STUBS));

    when(accountCredentialsRepoMock.getRequiredOne(ACCOUNT)).thenReturn(stackdriverCredentialsMock);

    // Create a time series with INT64 points
    List<Point> int64Points = new ArrayList<>();
    int64Points.add(
        Point.newBuilder()
            .setValue(TypedValue.newBuilder().setInt64Value(64l).build())
            .setInterval(
                TimeInterval.newBuilder()
                    .setStartTime(
                        Timestamp.newBuilder()
                            .setSeconds(Instant.parse("1970-01-01T00:00:00.00Z").getEpochSecond())
                            .build())
                    .setEndTime(
                        Timestamp.newBuilder()
                            .setSeconds(Instant.parse("1970-01-01T00:00:01.00Z").getEpochSecond())
                            .build())
                    .build())
            .build());
    List<TimeSeries> timeSeries =
        List.of(
            TimeSeries.newBuilder()
                .setValueType(MetricDescriptor.ValueType.INT64)
                .addAllPoints(int64Points)
                .build());

    ListTimeSeriesResponse timeSeriesListMock =
        ListTimeSeriesResponse.newBuilder().addAllTimeSeries(timeSeries).build();
    when(stackdriverCredentialsMock.getMonitoring().listTimeSeriesCallable().call(any()))
        .thenReturn(timeSeriesListMock);

    CanaryConfig canaryConfig = new CanaryConfig();
    CanaryMetricConfig canaryMetricConfig =
        CanaryMetricConfig.builder()
            .name("metricConfig")
            .query(
                StackdriverCanaryMetricSetQueryConfig.builder()
                    .resourceType("global")
                    .metricType("instance")
                    .build())
            .build();

    StackdriverCanaryScope canaryScope = new StackdriverCanaryScope();
    canaryScope.setStart(Instant.EPOCH).setEnd(Instant.EPOCH.plusSeconds(1)).setStep(1l);
    canaryScope.setProject("my-project");
    List<MetricSet> queriedMetrics =
        stackdriverMetricsService.queryMetrics(
            ACCOUNT, canaryConfig, canaryMetricConfig, canaryScope);
    System.out.println(queriedMetrics.get(0).getValues());
    assertThat(queriedMetrics.get(0).getValues()).contains(64d);
  }

  @Test
  void returnsSingleMetricDescriptorInCache() throws Exception {
    GoogleNamedAccountCredentials googleAccountCredentialsMock =
        new GoogleNamedAccountCredentials();
    googleAccountCredentialsMock.setName(ACCOUNT);
    mock(GoogleNamedAccountCredentials.class, Mockito.RETURNS_DEEP_STUBS);
    MetricServiceClient monitoringMock =
        mock(MetricServiceClient.class, Mockito.RETURNS_DEEP_STUBS);
    googleAccountCredentialsMock.setMonitoring(monitoringMock);

    when(accountCredentialsRepoMock.getAllOf(AccountCredentials.Type.METRICS_STORE))
        .thenReturn(Set.of(googleAccountCredentialsMock));

    // https://docs.cloud.google.com/java/docs/reference/google-cloud-monitoring/latest/com.google.monitoring.v3.ListTimeSeriesRequest.Builder#com_google_monitoring_v3_ListTimeSeriesRequest_Builder_setNameBytes_com_google_protobuf_ByteString_
    // The format is:
    //
    // projects/[PROJECT_ID_OR_NUMBER] organizations/[ORGANIZATION_ID] folders/[FOLDER_ID]
    MetricDescriptor metricDescriptor =
        MetricDescriptor.newBuilder().setName("project/" + ACCOUNT).build();
    List<MetricDescriptor> metricDesciprtorMockList = List.of(metricDescriptor);
    when(monitoringMock.listMetricDescriptorsCallable().call(any()).getMetricDescriptorsList())
        .thenReturn(metricDesciprtorMockList);

    stackdriverMetricsService.updateMetricDescriptorsCache();

    List<Map<String, ?>> metadata = stackdriverMetricsService.getMetadata(ACCOUNT, "");
    assertThat(metadata).hasSize(1);
    assertThat(metadata.get(0)).isEqualTo(PropertyUtils.describe(metricDescriptor));
  }

  @Test
  void handlesEmptyResponse() throws IOException {
    GoogleNamedAccountCredentials googleAccountCredentialsMock =
        new GoogleNamedAccountCredentials();
    googleAccountCredentialsMock.setName(ACCOUNT);
    mock(GoogleNamedAccountCredentials.class, Mockito.RETURNS_DEEP_STUBS);
    MetricServiceClient monitoringMock =
        mock(MetricServiceClient.class, Mockito.RETURNS_DEEP_STUBS);
    googleAccountCredentialsMock.setMonitoring(monitoringMock);

    when(accountCredentialsRepoMock.getRequiredOne(ACCOUNT))
        .thenReturn(googleAccountCredentialsMock);

    ListTimeSeriesResponse timeSeriesListMock = ListTimeSeriesResponse.newBuilder().build();
    when(monitoringMock.listTimeSeriesCallable().call(any())).thenReturn(timeSeriesListMock);
    // Return an empty list for time series

    CanaryConfig canaryConfig = new CanaryConfig();
    CanaryMetricConfig canaryMetricConfig =
        CanaryMetricConfig.builder()
            .name("metricConfig")
            .query(
                StackdriverCanaryMetricSetQueryConfig.builder()
                    .resourceType("global")
                    .metricType("instance")
                    .build())
            .build();

    StackdriverCanaryScope canaryScope = new StackdriverCanaryScope();
    canaryScope.setStart(Instant.EPOCH).setEnd(Instant.EPOCH.plusSeconds(1)).setStep(1l);
    canaryScope.setProject("my-project");
    List<MetricSet> queriedMetrics =
        stackdriverMetricsService.queryMetrics(
            ACCOUNT, canaryConfig, canaryMetricConfig, canaryScope);

    // Verify that an empty metric set is returned
    assertThat(queriedMetrics).hasSize(1);
    assertThat(queriedMetrics.get(0).getValues()).isEmpty();
  }

  @Test
  void handlesInvalidMetricType() throws IOException {
    GoogleNamedAccountCredentials googleAccountCredentialsMock =
        new GoogleNamedAccountCredentials();
    googleAccountCredentialsMock.setName(ACCOUNT);
    googleAccountCredentialsMock.setMonitoring(
        mock(MetricServiceClient.class, Mockito.RETURNS_DEEP_STUBS));
    when(accountCredentialsRepoMock.getRequiredOne(ACCOUNT))
        .thenReturn(googleAccountCredentialsMock);

    // Create a time series with an invalid metric type
    List<Point> points = new ArrayList<>();
    points.add(
        Point.newBuilder()
            .setValue(TypedValue.newBuilder().setDoubleValue(3.14).build())
            .setInterval(
                TimeInterval.newBuilder()
                    .setStartTime(
                        Timestamp.newBuilder()
                            .setSeconds(Instant.parse("1970-01-01T00:00:00.00Z").getEpochSecond())
                            .build())
                    .setEndTime(
                        Timestamp.newBuilder()
                            .setSeconds(Instant.parse("1970-01-01T00:00:01.00Z").getEpochSecond())
                            .build())
                    .build())
            .build());

    List<TimeSeries> timeSeriesListWithInvalidMetricType = new ArrayList<>();
    timeSeriesListWithInvalidMetricType.add(
        TimeSeries.newBuilder()
            .setValueType(MetricDescriptor.ValueType.STRING)
            .addAllPoints(points)
            .build());
    ListTimeSeriesResponse timeSeriesListMock =
        ListTimeSeriesResponse.newBuilder()
            .addAllTimeSeries(timeSeriesListWithInvalidMetricType)
            .build();
    when(googleAccountCredentialsMock.getMonitoring().listTimeSeriesCallable().call(any()))
        .thenReturn(timeSeriesListMock);

    CanaryConfig canaryConfig = new CanaryConfig();
    CanaryMetricConfig canaryMetricConfig =
        CanaryMetricConfig.builder()
            .name("metricConfig")
            .query(
                StackdriverCanaryMetricSetQueryConfig.builder()
                    .resourceType("global")
                    .metricType("instance")
                    .build())
            .build();

    StackdriverCanaryScope canaryScope = new StackdriverCanaryScope();
    canaryScope.setStart(Instant.EPOCH).setEnd(Instant.EPOCH.plusSeconds(1)).setStep(1l);
    canaryScope.setProject("my-project");
    List<MetricSet> queriedMetrics =
        stackdriverMetricsService.queryMetrics(
            ACCOUNT, canaryConfig, canaryMetricConfig, canaryScope);

    // Verify that the values are extracted as Double
    assertThat(queriedMetrics.get(0).getValues()).contains(3.14);
  }
}
