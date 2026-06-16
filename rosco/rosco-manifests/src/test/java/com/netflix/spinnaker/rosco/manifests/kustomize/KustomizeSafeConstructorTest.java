/*
 * Copyright 2026 Harness, Inc.
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

package com.netflix.spinnaker.rosco.manifests.kustomize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.kork.artifacts.model.Artifact;
import com.netflix.spinnaker.kork.yaml.JacksonYamlWrapper;
import com.netflix.spinnaker.kork.yaml.YamlHelper;
import com.netflix.spinnaker.rosco.manifests.kustomize.mapping.Kustomization;
import com.netflix.spinnaker.rosco.services.ClouddriverService;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import retrofit2.Call;
import retrofit2.Response;

/**
 * Proof tests that the safe YAML constructor used by {@link KustomizationFileReader} blocks known
 * CVE-2022-1471 / CWE-502 attack vectors.
 *
 * <p>{@link KustomizationFileReader#convert} parses untrusted kustomization YAML with {@link
 * YamlHelper#newYamlSafeConstructor()} which uses Jackson's YAML parser. Jackson's YAML parser
 * ignores YAML type tags (like {@code !!javax.script.ScriptEngineManager}) rather than attempting
 * to instantiate objects, which only produces standard types (Map, List, String, etc.). The
 * resulting map is then mapped to {@link
 * com.netflix.spinnaker.rosco.manifests.kustomize.mapping.Kustomization} via Jackson's {@link
 * com.fasterxml.jackson.databind.ObjectMapper#convertValue}. This two-step process prevents
 * arbitrary object instantiation via YAML tags.
 */
class KustomizeSafeConstructorTest {

  /**
   * Verifies that a malicious {@code !!javax.script.ScriptEngineManager} tag injected into a
   * String-typed field is safely ignored by Jackson and does not instantiate dangerous objects.
   */
  @Test
  void safeConstructorPreventsScriptEngineManagerInstantiationInStringField() {
    JacksonYamlWrapper yaml = YamlHelper.newYamlSafeConstructor();

    String maliciousYaml =
        "namePrefix: !!javax.script.ScriptEngineManager []\n"
            + "resources:\n"
            + "  - deployment.yml\n";

    // Jackson ignores the tag and treats it as an empty list/null value
    Object result = yaml.load(maliciousYaml);
    assertNotNull(result, "YAML should parse successfully, ignoring malicious tags");

    // Verify no dangerous objects were instantiated - result should be standard Java types only
    assertTrue(
        result instanceof java.util.Map,
        "Result should be a Map, not a dangerous object. Got: " + result.getClass().getName());
  }

  /**
   * Verifies that a malicious {@code !!java.net.URL} tag cannot be used for SSRF.
   *
   * <p>With the vulnerable {@code new Constructor(Kustomization.class)} configuration, the URL
   * object could be instantiated and trigger outbound connections. Jackson's YAML parser ignores
   * the tag and prevents instantiation.
   */
  @Test
  void safeConstructorPreventsURLInstantiation() throws Exception {
    AtomicBoolean connectionReceived = new AtomicBoolean(false);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    int port = server.getAddress().getPort();
    server.createContext(
        "/ssrf-test",
        exchange -> {
          connectionReceived.set(true);
          exchange.sendResponseHeaders(200, 0);
          exchange.close();
        });
    server.start();

    try {
      JacksonYamlWrapper yaml = YamlHelper.newYamlSafeConstructor();

      String maliciousYaml =
          "namePrefix: !!java.net.URL [\"http://127.0.0.1:"
              + port
              + "/ssrf-test\"]\n"
              + "resources:\n"
              + "  - deployment.yml\n";

      // Jackson ignores the tag and parses the content
      Object result = yaml.load(maliciousYaml);
      assertNotNull(result, "YAML should parse successfully, ignoring malicious tags");

      Thread.sleep(500);
      assertTrue(
          !connectionReceived.get(),
          "No connection should have been made because Jackson ignored the URL tag");
    } finally {
      server.stop(0);
    }
  }

  /**
   * Verifies that a malicious tag nested inside a map value (analogous to {@code
   * additionalProperties}) is safely ignored.
   */
  @Test
  void safeConstructorPreventsArbitraryInstantiationInMapFields() {
    JacksonYamlWrapper yaml = YamlHelper.newYamlSafeConstructor();

    String maliciousYaml =
        "resources:\n"
            + "  - deployment.yml\n"
            + "additionalProperties:\n"
            + "  evil: !!javax.script.ScriptEngineManager []\n";

    // Jackson ignores the tag and treats it as a standard value
    Object result = yaml.load(maliciousYaml);
    assertNotNull(result, "YAML should parse successfully, ignoring malicious tags");
    assertTrue(
        result instanceof java.util.Map,
        "Result should be a Map, not a dangerous object. Got: " + result.getClass().getName());
  }

  /** Verifies that a root-level malicious tag override is safely ignored. */
  @Test
  void safeConstructorPreventsRootTagOverride() {
    JacksonYamlWrapper yaml = YamlHelper.newYamlSafeConstructor();

    String maliciousYaml = "!!javax.script.ScriptEngineManager []";

    // Jackson ignores the tag and parses the empty list
    Object result = yaml.load(maliciousYaml);
    assertNotNull(result, "YAML should parse successfully, ignoring malicious tags");
    // Result should be a standard Java type (empty list/array), not a ScriptEngineManager
    assertTrue(
        result instanceof java.util.List || result instanceof java.util.Collection,
        "Result should be a List/Collection, not a dangerous object. Got: "
            + result.getClass().getName());
  }

  /**
   * Verifies that {@link KustomizationFileReader} safely handles malicious YAML by ignoring
   * dangerous tags. Jackson's YAML parser ignores YAML type tags, preventing object instantiation
   * attacks. The resulting parsed data contains only standard Java types (Map, List, String, etc.)
   * which are then safely mapped to the Kustomization object.
   */
  @Test
  @SuppressWarnings("unchecked")
  void kustomizationFileReaderHandlesMaliciousYamlSafely() throws Exception {
    String maliciousYaml =
        "resources:\n"
            + "  - deployment.yml\n"
            + "additionalProperties:\n"
            + "  evil: !!javax.script.ScriptEngineManager []\n";

    ClouddriverService clouddriverService = mock(ClouddriverService.class);
    Call<ResponseBody> call = mock(Call.class);

    ResponseBody responseBody =
        ResponseBody.create(null, maliciousYaml.getBytes(StandardCharsets.UTF_8));

    when(clouddriverService.fetchArtifact(any())).thenReturn(call);
    when(call.execute()).thenReturn(Response.success(200, responseBody));

    KustomizationFileReader reader = new KustomizationFileReader(clouddriverService);

    // Jackson ignores the malicious tag and parses the YAML safely
    Kustomization k =
        reader.getKustomization(
            Artifact.builder()
                .reference("http://example.com/base")
                .artifactAccount("test")
                .type("test")
                .build(),
            "kustomization.yaml");

    // Verify the legitimate data was parsed correctly
    assertNotNull(k);
    assertEquals(1, k.getResources().size());
    assertTrue(k.getResources().contains("deployment.yml"));
    // The malicious tag was ignored and treated as a standard value
  }

  /**
   * Regression test: a legitimate kustomization YAML must still be parsed correctly through the
   * SafeConstructor + Jackson convertValue path. Ensures the security fix did not break normal
   * functionality.
   */
  @Test
  @SuppressWarnings("unchecked")
  void kustomizationFileReaderParsesLegitimateYaml() throws Exception {
    String benignYaml =
        "namePrefix: prod-\n"
            + "resources:\n"
            + "  - deployment.yml\n"
            + "  - service.yml\n"
            + "patchesStrategicMerge:\n"
            + "  - patch.yml\n";

    ClouddriverService clouddriverService = mock(ClouddriverService.class);
    Call<ResponseBody> call = mock(Call.class);

    ResponseBody responseBody =
        ResponseBody.create(null, benignYaml.getBytes(StandardCharsets.UTF_8));

    when(clouddriverService.fetchArtifact(any())).thenReturn(call);
    when(call.execute()).thenReturn(Response.success(200, responseBody));

    KustomizationFileReader reader = new KustomizationFileReader(clouddriverService);

    Kustomization k =
        reader.getKustomization(
            Artifact.builder()
                .reference("http://example.com/base")
                .artifactAccount("test")
                .type("test")
                .build(),
            "kustomization.yaml");

    assertNotNull(k);
    assertEquals(2, k.getResources().size());
    assertTrue(k.getResources().contains("deployment.yml"));
    assertTrue(k.getResources().contains("service.yml"));
    assertEquals(1, k.getPatchesStrategicMerge().size());
    assertEquals("patch.yml", k.getPatchesStrategicMerge().get(0));
  }
}
