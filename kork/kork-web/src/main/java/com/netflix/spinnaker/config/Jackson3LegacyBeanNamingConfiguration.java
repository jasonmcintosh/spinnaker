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

import com.netflix.spinnaker.kork.jackson.LegacyAccessorNaming;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.MapperFeature;

/**
 * Keeps Jackson 2's bean property names on Boot-built Jackson 3 mappers.
 *
 * <p>Jackson 3 no longer lower-cases a run of leading capitals, so {@code getOAuthScopes()} moves
 * from the property {@code oauthScopes} to {@code OAuthScopes} (or {@code oAuthScopes} via {@code
 * FIX_FIELD_NAME_UPPER_CASE_PREFIX}). Data written by earlier releases - dynamic account
 * definitions, cache attributes, API request bodies - uses the old names and, with unknown
 * properties now ignored by default, would silently lose those values.
 *
 * <p>Set {@code spinnaker.jackson.legacy-bean-naming=false} to opt out once stored data and API
 * clients no longer depend on the old names. Other Jackson 3 default changes can be reverted with
 * Boot's own properties, for example {@code spring.jackson.deserialization.fail-on-null-for-
 * primitives=false} and {@code spring.jackson.mapper.use-getters-as-setters=true}.
 */
@Configuration
@ConditionalOnClass({JsonMapperBuilderCustomizer.class, tools.jackson.databind.ObjectMapper.class})
@ConditionalOnProperty(
    name = "spinnaker.jackson.legacy-bean-naming",
    havingValue = "true",
    matchIfMissing = true)
public class Jackson3LegacyBeanNamingConfiguration {

  @Bean
  public JsonMapperBuilderCustomizer legacyBeanNamingCustomizer() {
    return builder ->
        builder
            .accessorNaming(LegacyAccessorNaming.provider())
            .disable(MapperFeature.FIX_FIELD_NAME_UPPER_CASE_PREFIX);
  }
}
