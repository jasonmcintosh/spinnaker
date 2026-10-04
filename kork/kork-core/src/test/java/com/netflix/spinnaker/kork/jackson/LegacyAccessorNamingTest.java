/*
 * Copyright 2026 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.kork.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * The expected JSON below was produced by a stock Jackson 2.21 {@code ObjectMapper}. It pins the
 * property names that stored account definitions, cache attributes and API clients rely on.
 */
class LegacyAccessorNamingTest {

  private final ObjectMapper jackson3Default = JsonMapper.builder().build();

  private final ObjectMapper legacy =
      JsonMapper.builder()
          .accessorNaming(LegacyAccessorNaming.provider())
          .disable(MapperFeature.FIX_FIELD_NAME_UPPER_CASE_PREFIX)
          .build();

  /** Mirrors the Lombok shape of KubernetesAccountProperties.ManagedAccount. */
  public static class Account {
    private String name;
    private String oAuthServiceAccount;
    private List<String> oAuthScopes;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getOAuthServiceAccount() {
      return oAuthServiceAccount;
    }

    public void setOAuthServiceAccount(String value) {
      this.oAuthServiceAccount = value;
    }

    public List<String> getOAuthScopes() {
      return oAuthScopes;
    }

    public void setOAuthScopes(List<String> value) {
      this.oAuthScopes = value;
    }
  }

  public static class Acronyms {
    public String getURL() {
      return "u";
    }

    public String getARN() {
      return "a";
    }

    public String getXCoord() {
      return "x";
    }

    public String getaB() {
      return "ab";
    }

    public boolean isOK() {
      return true;
    }

    public String getFooBar() {
      return "f";
    }

    public String getAMIId() {
      return "i";
    }

    public int getIPCount() {
      return 1;
    }

    public String getvpcId() {
      return "v";
    }

    public String getS3Bucket() {
      return "b";
    }
  }

  @Test
  void jackson3DefaultRenamesAcronymProperties() throws Exception {
    // Documents the incompatibility this class exists to remove.
    Account account = new Account();
    account.setOAuthScopes(List.of("s1"));

    assertThat(jackson3Default.writeValueAsString(account)).doesNotContain("oauthScopes");
  }

  @Test
  void acronymAccessorsMatchJackson2() throws Exception {
    String json = legacy.writeValueAsString(new Acronyms());

    assertThat(json)
        .contains("\"url\":\"u\"", "\"arn\":\"a\"", "\"xcoord\":\"x\"", "\"aB\":\"ab\"")
        .contains("\"ok\":true", "\"fooBar\":\"f\"", "\"amiid\":\"i\"", "\"ipcount\":1")
        .contains("\"vpcId\":\"v\"", "\"s3Bucket\":\"b\"");
  }

  @Test
  void accountWritesAndReadsTheKeysJackson2Used() throws Exception {
    Account account = new Account();
    account.setName("n");
    account.setOAuthServiceAccount("sa");
    account.setOAuthScopes(List.of("s1"));

    // Property order is a separate concern (see Jackson3PropertyOrderConfiguration).
    assertThat(legacy.readTree(legacy.writeValueAsString(account)))
        .isEqualTo(
            legacy.readTree(
                "{\"name\":\"n\",\"oauthServiceAccount\":\"sa\",\"oauthScopes\":[\"s1\"]}"));

    Account read =
        legacy.readValue(
            "{\"name\":\"n\",\"oauthServiceAccount\":\"sa\",\"oauthScopes\":[\"s1\"]}",
            Account.class);
    assertThat(read.getOAuthServiceAccount()).isEqualTo("sa");
    assertThat(read.getOAuthScopes()).containsExactly("s1");
  }

  @Test
  void legacyJsonStoredByJackson2IsLostWithoutTheProvider() throws Exception {
    Account read =
        jackson3Default.readValue("{\"name\":\"n\",\"oauthScopes\":[\"s1\"]}", Account.class);

    assertThat(read.getOAuthScopes()).isNull();
  }

  /** AccountDefinitionMapper and similar callers derive their mapper with rebuild(). */
  @Test
  void rebuiltMappersKeepLegacyNaming() throws Exception {
    ObjectMapper derived = legacy.rebuild().enable(MapperFeature.USE_GETTERS_AS_SETTERS).build();
    Account account = new Account();
    account.setOAuthScopes(List.of("s1"));

    assertThat(derived.writeValueAsString(account)).contains("\"oauthScopes\"");
  }
}
