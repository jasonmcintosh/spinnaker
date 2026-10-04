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
package com.netflix.spinnaker.kork.yaml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.snakeyaml.engine.v2.api.LoadSettings;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;
import tools.jackson.dataformat.yaml.YAMLMapper;

class YamlHelperJacksonFactoryTest {

  private static final List<String> DOCUMENTS =
      List.of(
          "v:",
          "v: ",
          "v: ~",
          "v: null",
          "v: yes",
          "v: true",
          "v: 0644",
          "v: 1e3",
          "dup: 1\ndup: 2",
          "list:\n- a\n-\n- c",
          "nested:\n  empty:\n  other: x");

  private final ObjectMapper helperMapper =
      YAMLMapper.builder(new YamlHelper(new YamlParserProperties()).yamlFactory()).build();

  private final ObjectMapper plainMapper = YAMLMapper.builder().build();

  private static String read(ObjectMapper mapper, String doc) {
    try {
      return String.valueOf(mapper.readValue(doc, Object.class));
    } catch (JacksonException e) {
      return "ERROR " + e.getClass().getSimpleName();
    }
  }

  /** The reason this helper exists: hand-built LoadSettings change these two cases. */
  @Test
  void handBuiltLoadSettingsDiffersFromThePlainMapper() {
    ObjectMapper handBuilt =
        YAMLMapper.builder(
                YAMLFactory.builder()
                    .loadSettings(
                        LoadSettings.builder()
                            .setMaxAliasesForCollections(50)
                            .setCodePointLimit(3_145_728)
                            .build())
                    .build())
            .build();

    assertThat(read(handBuilt, "v:")).isEqualTo("{v=}");
    assertThat(read(handBuilt, "dup: 1\ndup: 2")).startsWith("ERROR");
  }

  @Test
  void helperFactoryReadsLikeThePlainMapper() {
    for (String doc : DOCUMENTS) {
      assertThat(read(helperMapper, doc)).as(doc).isEqualTo(read(plainMapper, doc));
    }
    assertThat(read(helperMapper, "v:")).isEqualTo("{v=null}");
    assertThat(read(helperMapper, "dup: 1\ndup: 2")).isEqualTo("{dup=2}");
  }

  @Test
  void codePointLimitIsStillEnforced() {
    YamlParserProperties props = new YamlParserProperties();
    props.setCodePointLimit(16);
    ObjectMapper limited = YAMLMapper.builder(new YamlHelper(props).yamlFactory()).build();

    assertThatThrownBy(() -> limited.readValue("key: " + "x".repeat(64), Object.class))
        .isInstanceOf(JacksonException.class);
  }
}
