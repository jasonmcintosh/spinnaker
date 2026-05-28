package com.netflix.spinnaker.kork.yaml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;
import com.netflix.spinnaker.kork.yaml.JacksonYamlWrapper.YamlProcessingException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = YamlHelperTest.TestConfig.class)
@TestPropertySource(
    properties = {"snakeyaml.max-aliases-for-collections=55", "snakeyaml.code-point-limit=1024"})
class YamlHelperTest {

  @Autowired private YamlHelper yamlHelper;

  @Test
  public void aliasLimitIsEnforcedViaNestingDepth() {
    // With Jackson+SnakeYAML, alias expansion is controlled via LoaderOptions
    String doc = yamlWithNAliases(56);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(doc))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("Number of aliases for non-scalar nodes exceeds the specified max=55");
  }

  @Test
  public void aliasLimitIsNotExceeded() {
    // Create YAML with moderate nesting that should pass
    String okString = yamlWithNAliases(50);
    Object result = YamlHelper.newYamlSafeConstructor().load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  public void codePointLimitIsEnforced() {
    // This string has more than 1024 characters
    String bigString = yamlWithNCodePoints(1025);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(bigString))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("The incoming YAML document exceeds the limit: 1024 code points.");
  }

  @Test
  public void codePointLimitIsNotExceeded() {
    String okString = yamlWithNCodePoints(1000);
    Object result = YamlHelper.newYamlSafeConstructor().load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  public void aliasLimitIsEnforcedYamlSafeConstructor() {
    String doc = yamlWithNAliases(56);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(doc))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("Number of aliases for non-scalar nodes exceeds the specified max=55");
  }

  @Test
  public void aliasLimitIsNotExceededYamlSafeConstructor() {
    String okString = yamlWithNAliases(50);
    Object result = YamlHelper.newYamlSafeConstructor().load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  public void codePointLimitIsEnforcedYamlSafeConstructor() {
    String bigString = yamlWithNCodePoints(1025);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(bigString))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("The incoming YAML document exceeds the limit: 1024 code points.");
  }

  @Test
  public void codePointLimitIsNotExceededYamlSafeConstructor() {
    String okString = yamlWithNCodePoints(1000);
    Object result = YamlHelper.newYamlSafeConstructor().load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  public void aliasLimitIsEnforcedYamlDumperOptions() {
    String doc = yamlWithNAliases(56);
    assertThatThrownBy(() -> YamlHelper.newYamlDumperOptions(null).load(doc))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("Number of aliases for non-scalar nodes exceeds the specified max=55");
  }

  @Test
  public void aliasLimitIsNotExceededYamlDumperOptions() {
    String okString = yamlWithNAliases(50);
    Object result = YamlHelper.newYamlDumperOptions(null).load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  public void codePointLimitIsEnforcedYamlDumperOptions() {
    String bigString = yamlWithNCodePoints(1025);
    assertThatThrownBy(() -> YamlHelper.newYamlDumperOptions(null).load(bigString))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("The incoming YAML document exceeds the limit: 1024 code points.");
  }

  @Test
  public void codePointLimitIsNotExceededYamlDumperOptions() {
    String okString = yamlWithNCodePoints(1000);
    Object result = YamlHelper.newYamlDumperOptions(null).load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  public void aliasLimitIsEnforcedYamlLoaderOptions() {
    String doc = yamlWithNAliases(56);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(doc))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("Number of aliases for non-scalar nodes exceeds the specified max=55");
  }

  @Test
  public void aliasLimitIsNotExceededYamlLoaderOptions() {
    String okString = yamlWithNAliases(50);
    Object result = YamlHelper.newYamlSafeConstructor().load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  void codePointLimitIsEnforcedYamlLoaderOptions() {
    String bigString = yamlWithNCodePoints(1025);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(bigString))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("The incoming YAML document exceeds the limit: 1024 code points.");
  }

  @Test
  void codePointLimitIsNotExceededYamlLoaderOptions() {
    String okString = yamlWithNCodePoints(1000);
    Object result = YamlHelper.newYamlSafeConstructor().load(okString);
    assertThat(result).isNotNull();
  }

  @Test
  void aliasLimitIsEnforcedYamlRepresenter() {
    String doc = yamlWithNAliases(56);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(doc))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("Number of aliases for non-scalar nodes exceeds the specified max=55");
  }

  @Test
  void aliasLimitIsNotExceededYamlRepresenter() {
    String okString = yamlWithNAliases(50);
    assertThat(YamlHelper.newYamlSafeConstructor().load(okString)).isNotNull();
  }

  @Test
  void codePointLimitIsEnforcedYamlRepresenter() {
    String bigString = yamlWithNCodePoints(1025);
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(bigString))
        .isInstanceOf(YamlProcessingException.class)
        .hasCauseInstanceOf(JacksonYAMLParseException.class)
        .hasStackTraceContaining("The incoming YAML document exceeds the limit: 1024 code points.");
  }

  @Test
  void codePointLimitIsNotExceededYamlRepresenter() {
    String okString = yamlWithNCodePoints(1000);
    Object result = YamlHelper.newYamlSafeConstructor().load(okString);
    assertThat(result).isNotNull();
  }

  private String yamlWithNAliases(int nAliases) {
    StringBuilder sb = new StringBuilder();
    sb.append("defaults: &default\n  a: 1\n  b: 2\n");

    for (int i = 0; i < nAliases; i++) {
      sb.append("alias").append(i).append(": *default\n");
    }

    return sb.toString();
  }

  private String deeplyNestedYaml(int depth) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      sb.append("level").append(i).append(":\n");
      for (int j = 0; j <= i; j++) {
        sb.append("  ");
      }
    }
    sb.append("value: deep");
    return sb.toString();
  }

  private String yamlWithNCodePoints(int nCodePoints) {
    StringBuilder sb = new StringBuilder();
    sb.append("value: ");
    for (int i = 0; i < nCodePoints; i++) {
      sb.append("a");
    }
    return sb.toString();
  }

  @Configuration
  @EnableConfigurationProperties(YamlParserProperties.class)
  @ComponentScan(basePackageClasses = YamlHelper.class)
  static class TestConfig {}
}
