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
package com.netflix.spinnaker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import tools.jackson.databind.json.JsonMapper;

class Jackson3LegacyBeanNamingConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
          .withUserConfiguration(
              Jackson3PropertyOrderConfiguration.class,
              Jackson3LegacyBeanNamingConfiguration.class);

  public static class Account {
    private String oAuthServiceAccount;
    private List<String> oAuthScopes;
    private final List<String> labels = new ArrayList<>();
    private int count;

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

    public List<String> getLabels() {
      return labels;
    }

    public int getCount() {
      return count;
    }

    public void setCount(int count) {
      this.count = count;
    }
  }

  @Test
  void bootMapperUsesJackson2PropertyNames() {
    runner.run(
        ctx -> {
          JsonMapper mapper = ctx.getBean(JsonMapper.class);
          Account account = new Account();
          account.setOAuthScopes(List.of("s1"));

          assertThat(mapper.writeValueAsString(account)).contains("\"oauthScopes\":[\"s1\"]");
          Account read = mapper.readValue("{\"oauthScopes\":[\"s2\"]}", Account.class);
          assertThat(read.getOAuthScopes()).containsExactly("s2");
        });
  }

  @Test
  void canBeDisabled() {
    runner
        .withPropertyValues("spinnaker.jackson.legacy-bean-naming=false")
        .run(ctx -> assertThat(ctx).doesNotHaveBean("legacyBeanNamingCustomizer"));
  }

  /** Other Jackson 3 default changes are reverted with Boot's standard properties. */
  @Test
  void bootPropertiesRevertOtherJackson3Defaults() {
    runner
        .withPropertyValues(
            "spring.jackson.deserialization.fail-on-null-for-primitives=false",
            "spring.jackson.deserialization.fail-on-trailing-tokens=false",
            "spring.jackson.mapper.use-getters-as-setters=true")
        .run(
            ctx -> {
              JsonMapper mapper = ctx.getBean(JsonMapper.class);

              assertThat(mapper.readValue("{\"count\":null}", Account.class).getCount()).isZero();
              assertThat(mapper.readValue("{\"count\":1} trailing", Account.class).getCount())
                  .isEqualTo(1);
              assertThat(mapper.readValue("{\"labels\":[\"a\"]}", Account.class).getLabels())
                  .containsExactly("a");
            });
  }
}
