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
package com.netflix.spinnaker.clouddriver.kubernetes.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.spinnaker.clouddriver.jackson.AccountDefinitionModule;
import com.netflix.spinnaker.credentials.definition.CredentialsDefinition;
import com.netflix.spinnaker.kork.jackson.LegacyAccessorNaming;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.jsontype.NamedType;

/**
 * Managed accounts are stored (SQL account store) and accepted over {@code POST /credentials} as
 * JSON. The OAuth settings were bound as {@code oauthServiceAccount} / {@code oauthScopes} under
 * Jackson 2; Jackson 3 renames them unless legacy bean naming is applied (kork-web does that for
 * Boot mappers via {@code spinnaker.jackson.legacy-bean-naming}).
 */
class KubernetesManagedAccountJsonTest {

  private static final String STORED_BY_JACKSON_2 =
      "{\"type\":\"kubernetes\",\"name\":\"gke\",\"oauthServiceAccount\":\"sa@example\","
          + "\"oauthScopes\":[\"scope-a\",\"scope-b\"]}";

  private static ObjectMapper mapper(boolean legacyNaming) {
    var builder =
        JsonMapper.builder()
            .enable(MapperFeature.USE_GETTERS_AS_SETTERS)
            .addModule(
                new AccountDefinitionModule(
                    new NamedType(KubernetesAccountProperties.ManagedAccount.class, "kubernetes")));
    if (legacyNaming) {
      builder
          .accessorNaming(LegacyAccessorNaming.provider())
          .disable(MapperFeature.FIX_FIELD_NAME_UPPER_CASE_PREFIX);
    }
    return builder.build();
  }

  @Test
  void legacyJsonStoredByJackson2BindsOAuthSettings() throws Exception {
    var account =
        (KubernetesAccountProperties.ManagedAccount)
            mapper(true).readValue(STORED_BY_JACKSON_2, CredentialsDefinition.class);

    assertThat(account.getOAuthServiceAccount()).isEqualTo("sa@example");
    assertThat(account.getOAuthScopes()).containsExactly("scope-a", "scope-b");
  }

  @Test
  void serializedAccountKeepsTheJackson2KeysSoOlderReplicasCanReadIt() throws Exception {
    var account = new KubernetesAccountProperties.ManagedAccount();
    account.setName("gke");
    account.setOAuthServiceAccount("sa@example");
    account.setOAuthScopes(List.of("scope-a"));

    JsonNode json = mapper(true).readTree(mapper(true).writeValueAsString(account));

    assertThat(json.has("oauthServiceAccount")).isTrue();
    assertThat(json.get("oauthScopes").get(0).asString()).isEqualTo("scope-a");
    assertThat(json.has("oAuthScopes")).isFalse();
    assertThat(json.has("OAuthScopes")).isFalse();
  }

  @Test
  void roundTripPreservesTheAccount() throws Exception {
    ObjectMapper mapper = mapper(true);
    var account = new KubernetesAccountProperties.ManagedAccount();
    account.setName("gke");
    account.setOAuthServiceAccount("sa@example");
    account.setOAuthScopes(List.of("scope-a"));

    var read =
        (KubernetesAccountProperties.ManagedAccount)
            mapper.readValue(mapper.writeValueAsString(account), CredentialsDefinition.class);

    assertThat(read).isEqualTo(account);
  }

  @Test
  void withoutLegacyNamingTheOAuthSettingsAreSilentlyDropped() throws Exception {
    // Documents the regression: no error, the account just loses its OAuth configuration.
    var account =
        (KubernetesAccountProperties.ManagedAccount)
            mapper(false).readValue(STORED_BY_JACKSON_2, CredentialsDefinition.class);

    assertThat(account.getOAuthServiceAccount()).isNull();
    assertThat(account.getOAuthScopes()).isNull();
  }
}
