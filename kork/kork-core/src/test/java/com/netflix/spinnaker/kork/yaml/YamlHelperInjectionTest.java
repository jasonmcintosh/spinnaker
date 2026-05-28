package com.netflix.spinnaker.kork.yaml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

/**
 * Security tests demonstrating YAML injection vulnerabilities.
 *
 * <p>These tests demonstrate potential injection attacks against YamlHelper, specifically showing
 * how Jackson's YAML parser is secure by default and doesn't support arbitrary Java object
 * instantiation through YAML tags like SnakeYAML's unsafe Constructor.
 *
 * <p>Jackson's YAMLMapper provides built-in security by:
 *
 * <ul>
 *   <li>Not supporting arbitrary Java type tags (!!java.net.URL, etc.) by default
 *   <li>Using stream constraints to limit document size and complexity
 *   <li>Preventing arbitrary object instantiation without explicit configuration
 * </ul>
 */
@SpringBootTest(classes = YamlHelperInjectionTest.TestConfig.class)
@TestPropertySource(
    properties = {"snakeyaml.max-aliases-for-collections=50", "snakeyaml.code-point-limit=10000"})
class YamlHelperInjectionTest {

  /**
   * Demonstrates that Jackson blocks arbitrary object instantiation.
   *
   * <p>Unlike SnakeYAML's unsafe Constructor, Jackson doesn't support Java type tags by default,
   * making it secure against arbitrary object instantiation attacks.
   */
  @Test
  public void safeConstructorBlocksArbitraryObjectInstantiation() {
    String maliciousYaml =
        """
            !!java.net.URL ["http://malicious.example.com/"]
            """;

    // Jackson blocks arbitrary object instantiation by not recognizing Java type tags - SECURE
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(maliciousYaml))
        .isInstanceOf(MismatchedInputException.class);
  }

  /** Demonstrates that Jackson blocks ScriptEngineManager instantiation. */
  @Test
  public void safeConstructorBlocksScriptEngineManagerInstantiation() {
    String maliciousYaml = """
            !!javax.script.ScriptEngineManager []
            """;

    // Jackson blocks ScriptEngineManager instantiation - SECURE
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(maliciousYaml))
        .isInstanceOf(MismatchedInputException.class);
  }

  /** Demonstrates that Jackson blocks tag injection in map keys. */
  @Test
  public void safeConstructorBlocksTagInjectionInMapKeys() {
    String maliciousYaml =
        """
            ? !!java.net.URL ["http://evil.com/"]
            : value
            """;

    // Jackson blocks object instantiation in keys - SECURE
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(maliciousYaml))
        .isInstanceOf(MismatchedInputException.class);
  }

  /**
   * Demonstrates YAML bomb (Billion Laughs) attack vector.
   *
   * <p>YamlHelper uses Jackson's stream constraints to limit nesting depth, which protects against
   * YAML bombs that use entity expansion to cause exponential memory consumption.
   */
  @Test
  public void demonstratesYamlBombAttackIsBlocked() {
    // Create a deeply nested YAML structure that exceeds reasonable limits
    StringBuilder yamlBomb = new StringBuilder();

    // Create nested structure that will exceed nesting depth limit
    for (int i = 0; i < 60; i++) {
      yamlBomb.append("level").append(i).append(":\n");
      for (int j = 0; j <= i; j++) {
        yamlBomb.append("  ");
      }
    }
    yamlBomb.append("value: bomb");

    String bomb = yamlBomb.toString();

    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(bomb))
        .isInstanceOf(StreamConstraintsException.class)
        .hasMessageContaining("exceeds the maximum allowed");
  }

  /** Demonstrates that Jackson blocks nested object instantiation. */
  @Test
  public void safeConstructorBlocksNestedObjectInstantiationAttack() {
    String maliciousYaml =
        """
          application:
            name: myapp
            config:
              url: !!java.net.URL ["http://attacker.com/exfiltrate"]
          """;

    // Jackson blocks nested arbitrary objects - SECURE
    assertThatThrownBy(() -> YamlHelper.newYamlSafeConstructor().load(maliciousYaml))
        .isInstanceOf(MismatchedInputException.class);
  }

  /**
   * Demonstrates that newYamlDumperOptions also uses Jackson and is secure.
   *
   * <p>Verifies that all YamlHelper methods consistently use Jackson's secure YAML parser.
   */
  @Test
  public void newYamlDumperOptionsBlocksArbitraryObjectInstantiation() {
    String maliciousYaml =
        """
          !!java.net.URL ["http://malicious.example.com/"]
          """;

    // newYamlDumperOptions() uses Jackson - SECURE
    assertThatThrownBy(() -> YamlHelper.newYamlDumperOptions(null).load(maliciousYaml))
        .isInstanceOf(MismatchedInputException.class);
  }

  @Test
  public void canParseValidYamlStructures() {
    String validYaml =
        """
        application:
          name: myapp
          version: 1.0.0
          config:
            timeout: 30
            enabled: true
        """;

    Object result = YamlHelper.newYamlSafeConstructor().load(validYaml);
    assertThat(result).isNotNull();
    assertThat(result).isInstanceOf(java.util.Map.class);
  }

  @Configuration
  @EnableConfigurationProperties(YamlParserProperties.class)
  @ComponentScan(basePackageClasses = YamlHelper.class)
  static class TestConfig {}
}
