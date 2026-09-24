/*
 * Copyright 2025 OpsMx, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.igor.jenkins.service;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.netflix.spinnaker.fiat.model.resources.Permissions;
import com.netflix.spinnaker.igor.exceptions.BuildJobError;
import com.netflix.spinnaker.igor.jenkins.client.JenkinsClient;
import com.netflix.spinnaker.igor.model.Crumb;
import com.netflix.spinnaker.kork.retrofit.ErrorHandlingExecutorCallAdapterFactory;
import com.netflix.spinnaker.kork.retrofit.util.RetrofitUtils;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.util.Collections;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class JenkinsServiceTest {

  @RegisterExtension
  static final WireMockExtension wmJenkins =
      WireMockExtension.newInstance().options(wireMockConfig().dynamicPort()).build();

  static JenkinsClient jenkinsClient;
  static JenkinsService jenkinsService;
  static ObjectMapper objectMapper;

  @BeforeAll
  public static void setup() {
    objectMapper =
        new XmlMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModule(new JaxbAnnotationModule());
    jenkinsClient =
        new Retrofit.Builder()
            .baseUrl(RetrofitUtils.getBaseUrl(wmJenkins.baseUrl()))
            // Mirrors JenkinsConfig's production client: redirects must be disabled so a 303
            // (already queued/running) surfaces as a SpinnakerHttpException instead of being
            // silently followed into an indistinguishable 200.
            .client(new OkHttpClient.Builder().followRedirects(false).build())
            .addCallAdapterFactory(ErrorHandlingExecutorCallAdapterFactory.getInstance())
            .addConverterFactory(JacksonConverterFactory.create(objectMapper))
            .build()
            .create(JenkinsClient.class);
    CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
    jenkinsService =
        new JenkinsService(
            RetrofitUtils.getBaseUrl(wmJenkins.baseUrl()),
            jenkinsClient,
            true,
            Permissions.EMPTY,
            circuitBreakerRegistry);
  }

  @Test
  public void testJenkinsJobBuild() throws JsonProcessingException {
    Crumb crumb = new Crumb();
    crumb.setCrumb("crumb");
    wmJenkins.stubFor(
        WireMock.get("/crumbIssuer/api/xml")
            .willReturn(WireMock.aResponse().withBody(objectMapper.writeValueAsString(crumb))));

    wmJenkins.stubFor(
        WireMock.post("/job/job1/build").willReturn(WireMock.aResponse().withStatus(201)));

    jenkinsService.build("job1");

    wmJenkins.verify(1, WireMock.getRequestedFor(WireMock.urlEqualTo("/crumbIssuer/api/xml")));
    wmJenkins.verify(1, WireMock.postRequestedFor(WireMock.urlEqualTo("/job/job1/build")));
  }

  @Test
  public void triggerBuildWithParametersReturnsQueueIdOnSuccessfulSubmission()
      throws JsonProcessingException {
    Crumb crumb = new Crumb();
    crumb.setCrumb("crumb");
    wmJenkins.stubFor(
        WireMock.get("/crumbIssuer/api/xml")
            .willReturn(WireMock.aResponse().withBody(objectMapper.writeValueAsString(crumb))));

    wmJenkins.stubFor(
        WireMock.post(WireMock.urlPathEqualTo("/job/job2/buildWithParameters"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(201)
                    .withHeader("location", wmJenkins.baseUrl() + "/queue/item/42")));

    long queueId =
        jenkinsService.triggerBuildWithParameters("job2", Collections.singletonMap("foo", "bar"));

    assertThat(queueId).isEqualTo(42L);
  }

  @Test
  public void triggerBuildWithParametersResolvesExistingQueueItemOn303()
      throws JsonProcessingException {
    Crumb crumb = new Crumb();
    crumb.setCrumb("crumb");
    wmJenkins.stubFor(
        WireMock.get("/crumbIssuer/api/xml")
            .willReturn(WireMock.aResponse().withBody(objectMapper.writeValueAsString(crumb))));

    wmJenkins.stubFor(
        WireMock.post(WireMock.urlPathEqualTo("/job/job3/buildWithParameters"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(303)
                    .withHeader("location", wmJenkins.baseUrl() + "/queue/item/99")));

    long queueId =
        jenkinsService.triggerBuildWithParameters("job3", Collections.singletonMap("foo", "bar"));

    assertThat(queueId).isEqualTo(99L);
  }

  @Test
  public void triggerBuildWithParametersThrowsOnUnexpectedStatus() throws JsonProcessingException {
    Crumb crumb = new Crumb();
    crumb.setCrumb("crumb");
    wmJenkins.stubFor(
        WireMock.get("/crumbIssuer/api/xml")
            .willReturn(WireMock.aResponse().withBody(objectMapper.writeValueAsString(crumb))));

    wmJenkins.stubFor(
        WireMock.post(WireMock.urlPathEqualTo("/job/job4/buildWithParameters"))
            .willReturn(WireMock.aResponse().withStatus(200)));

    assertThatThrownBy(
            () ->
                jenkinsService.triggerBuildWithParameters(
                    "job4", Collections.singletonMap("foo", "bar")))
        .isInstanceOf(BuildJobError.class);
  }
}
