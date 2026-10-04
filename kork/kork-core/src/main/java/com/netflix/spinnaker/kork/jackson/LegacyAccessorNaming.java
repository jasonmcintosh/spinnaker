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

import tools.jackson.databind.cfg.MapperConfig;
import tools.jackson.databind.introspect.AccessorNamingStrategy;
import tools.jackson.databind.introspect.AnnotatedClass;
import tools.jackson.databind.introspect.DefaultAccessorNamingStrategy;

/**
 * Restores Jackson 2's default bean property naming on Jackson 3 mappers.
 *
 * <p>Jackson 2 lower-cased the whole leading run of capitals in an accessor name, so {@code
 * getOAuthScopes()} was the property {@code oauthScopes} and {@code getURL()} was {@code url}.
 * Jackson 3 uses standard bean naming and keeps them ({@code OAuthScopes}, {@code URL}), and its
 * {@code FIX_FIELD_NAME_UPPER_CASE_PREFIX} feature can pick yet another name from a matching field.
 * Because Jackson 3 also no longer fails on unknown properties, JSON that was persisted or sent by
 * Jackson 2 era code (account definitions, cache attributes, API clients) silently stops binding.
 *
 * <p>Use with {@code builder.accessorNaming(LegacyAccessorNaming.provider())} and disable {@code
 * MapperFeature.FIX_FIELD_NAME_UPPER_CASE_PREFIX}.
 */
public final class LegacyAccessorNaming {

  private LegacyAccessorNaming() {}

  public static AccessorNamingStrategy.Provider provider() {
    return new LegacyProvider();
  }

  /** Jackson 2 accepted accessors such as {@code getvpcId()}; Jackson 3 requires an upper case. */
  private static final class LegacyProvider extends DefaultAccessorNamingStrategy.Provider {
    LegacyProvider() {
      super(
          "set",
          "with",
          "get",
          "is",
          DefaultAccessorNamingStrategy.FirstCharBasedValidator.forFirstNameRule(true, true));
    }

    @Override
    public AccessorNamingStrategy forPOJO(MapperConfig<?> config, AnnotatedClass targetClass) {
      return new LegacyStrategy(
          config, targetClass, _setterPrefix, _getterPrefix, _isGetterPrefix, _baseNameValidator);
    }
  }

  private static final class LegacyStrategy extends DefaultAccessorNamingStrategy {
    LegacyStrategy(
        MapperConfig<?> config,
        AnnotatedClass forClass,
        String mutatorPrefix,
        String getterPrefix,
        String isGetterPrefix,
        BaseNameValidator baseNameValidator) {
      super(config, forClass, mutatorPrefix, getterPrefix, isGetterPrefix, baseNameValidator);
    }

    @Override
    protected String stdManglePropertyName(String basename, int offset) {
      return legacyManglePropertyName(basename, offset);
    }
  }

  /** Port of Jackson 2's {@code BeanUtil.legacyManglePropertyName}. */
  static String legacyManglePropertyName(String basename, int offset) {
    int end = basename.length();
    if (end == offset) {
      return null;
    }
    char c = basename.charAt(offset);
    char d = Character.toLowerCase(c);
    if (c == d) {
      return basename.substring(offset);
    }
    StringBuilder sb = new StringBuilder(end - offset);
    sb.append(d);
    for (int i = offset + 1; i < end; ++i) {
      c = basename.charAt(i);
      d = Character.toLowerCase(c);
      if (c == d) {
        sb.append(basename, i, end);
        break;
      }
      sb.append(d);
    }
    return sb.toString();
  }
}
