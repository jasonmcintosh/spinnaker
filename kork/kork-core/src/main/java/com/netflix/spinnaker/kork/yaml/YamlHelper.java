package com.netflix.spinnaker.kork.yaml;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactoryBuilder;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;

/**
 * Utility component for creating preconfigured Jackson-based YAML instances with optional
 * security-related parsing limits.
 *
 * <p>This helper centralizes the creation of YAML parser objects used across the Spinnaker
 * ecosystem, ensuring that YAML parsing behavior is consistent and secure. It applies limits
 * defined in {@link YamlParserProperties}, such as:
 *
 * <ul>
 *   <li>{@code maxAliasesForCollections} – to prevent Billion Laughs (entity expansion) attacks
 *   <li>{@code codePointLimit} – to restrict the maximum size of YAML input
 * </ul>
 *
 * <p>This implementation uses Jackson's YAML dataformat library instead of SnakeYAML, providing
 * better integration with the Jackson ecosystem and improved security controls.
 *
 * <p>If no security-related properties are configured, the helper falls back to creating standard
 * YAML parser instances using Jackson defaults.
 *
 * <p><strong>Usage Example:</strong>
 *
 * <pre>{@code
 * JacksonYamlWrapper yaml = YamlHelper.newYaml();
 * Map<String, Object> data = (Map<String, Object>) yaml.load(yamlContent);
 * }</pre>
 *
 * <p>When {@link YamlParserProperties} is available in the Spring context, security properties are
 * automatically applied to all created YAML parser instances.
 */
@Component
@Log4j2
public class YamlHelper {

  private static YamlParserProperties yamlParserProperties;

  @Autowired
  public YamlHelper(YamlParserProperties props) {
    yamlParserProperties = props;
  }

  private static boolean hasYamlSecurityPropertiesConfigured() {
    return yamlParserProperties != null
        && (yamlParserProperties.getMaxAliasesForCollections() != null
            || yamlParserProperties.getCodePointLimit() != null);
  }

  /**
   * Creates a new {@link JacksonYamlWrapper} instance with safe construction for a specific class.
   *
   * @return a new YAML wrapper instance
   */
  public static JacksonYamlWrapper newYamlSafeConstructor() {
    YAMLFactory factory = createYamlFactory();
    applyStreamConstraints(factory);
    ObjectMapper mapper = new ObjectMapper(factory);
    return new JacksonYamlWrapper(mapper);
  }

  /**
   * Creates a new {@link JacksonYamlWrapper} instance with default YAML generation options.
   *
   * <p>Note: The dumperOptions parameter is not directly used in Jackson but this method is kept
   * for API compatibility with the SnakeYAML-based implementation.
   *
   * @param dumperOptions ignored (kept for API compatibility)
   * @return a new YAML wrapper instance
   */
  public static JacksonYamlWrapper newYamlDumperOptions(DumperOptions dumperOptions) {
    YAMLFactory factory = createYamlFactory(dumperOptions);
    applyStreamConstraints(factory);
    ObjectMapper mapper = new ObjectMapper(factory);
    return new JacksonYamlWrapper(mapper);
  }

  /**
   * Creates a new {@link JacksonYamlWrapper} instance with specified loader options.
   *
   * <p>Note: The loaderOptions parameter is not directly used in Jackson but this method is kept
   * for API compatibility. Security properties are still applied if configured.
   *
   * @param loaderOptions ignored (kept for API compatibility)
   * @return a new YAML wrapper instance
   */
  public static JacksonYamlWrapper newYamlLoaderOptions(LoaderOptions loaderOptions) {
    YAMLFactory factory = createYamlFactory(loaderOptions);
    applyStreamConstraints(factory);
    ObjectMapper mapper = new ObjectMapper(factory);
    return new JacksonYamlWrapper(mapper);
  }

  /**
   * Creates a configured YAMLFactory with appropriate settings.
   *
   * @return a new YAMLFactory instance
   */
  private static YAMLFactory createYamlFactory() {
    LoaderOptions loaderOptions = null;
    if (hasYamlSecurityPropertiesConfigured()) {
      loaderOptions = new LoaderOptions();
      if (yamlParserProperties.getMaxAliasesForCollections() != null) {
        loaderOptions.setMaxAliasesForCollections(
            yamlParserProperties.getMaxAliasesForCollections());
      }
      if (yamlParserProperties.getCodePointLimit() != null) {
        loaderOptions.setCodePointLimit(yamlParserProperties.getCodePointLimit());
      }
    }

    YAMLFactoryBuilder builder =
        YAMLFactory.builder()
            .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR);

    if (loaderOptions != null) {
      builder.loaderOptions(loaderOptions);
    }

    return builder.build();
  }

  private static YAMLFactory createYamlFactory(DumperOptions dumperOptions) {
    LoaderOptions loaderOptions = null;
    if (hasYamlSecurityPropertiesConfigured()) {
      loaderOptions = new LoaderOptions();
      if (yamlParserProperties.getMaxAliasesForCollections() != null) {
        loaderOptions.setMaxAliasesForCollections(
            yamlParserProperties.getMaxAliasesForCollections());
      }
      if (yamlParserProperties.getCodePointLimit() != null) {
        loaderOptions.setCodePointLimit(yamlParserProperties.getCodePointLimit());
      }
    }

    YAMLFactoryBuilder builder =
        YAMLFactory.builder()
            .dumperOptions(dumperOptions)
            .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR);

    if (loaderOptions != null) {
      builder.loaderOptions(loaderOptions);
    }

    return builder.build();
  }

  private static YAMLFactory createYamlFactory(LoaderOptions loaderOptions) {
    // Apply security constraints from properties if configured
    if (hasYamlSecurityPropertiesConfigured() && loaderOptions != null) {
      if (yamlParserProperties.getMaxAliasesForCollections() != null) {
        loaderOptions.setMaxAliasesForCollections(
            yamlParserProperties.getMaxAliasesForCollections());
      }
      if (yamlParserProperties.getCodePointLimit() != null) {
        loaderOptions.setCodePointLimit(yamlParserProperties.getCodePointLimit());
      }
    }

    return YAMLFactory.builder()
        .loaderOptions(loaderOptions)
        .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
        .enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR)
        .build();
  }

  /**
   * Applies security-related stream constraints to the YAMLFactory based on configured properties.
   *
   * @param factory the YAMLFactory to configure
   */
  private static void applyStreamConstraints(YAMLFactory factory) {
    if (!hasYamlSecurityPropertiesConfigured()) {
      return;
    }

    StreamReadConstraints.Builder constraintsBuilder = StreamReadConstraints.builder();

    if (yamlParserProperties.getCodePointLimit() != null) {
      long limit = yamlParserProperties.getCodePointLimit();
      // Jackson uses maxStringLength for similar protection
      constraintsBuilder.maxStringLength((int) limit);
      log.debug("Applied YAML code point limit: {}", limit);
    }

    if (yamlParserProperties.getMaxAliasesForCollections() != null) {
      int maxAliases = yamlParserProperties.getMaxAliasesForCollections();
      // Jackson doesn't have a direct equivalent, but we can limit nesting depth
      // as a proxy for protection against alias expansion attacks
      constraintsBuilder.maxNestingDepth(Math.min(maxAliases, 1000));
      log.debug("Applied YAML max aliases limit (via nesting depth): {}", maxAliases);
    }

    factory.setStreamReadConstraints(constraintsBuilder.build());
  }

  /**
   * Gets loader options configuration. This method is kept for API compatibility but returns a
   * no-op object since Jackson handles constraints differently.
   *
   * @param opts ignored
   * @return the input object (for API compatibility)
   * @deprecated This method is kept for backward compatibility but has no effect with Jackson
   */
  @Deprecated
  public static Object getLoaderOptions(Object opts) {
    // Return the input for API compatibility
    // Actual constraints are applied via StreamReadConstraints in Jackson
    return opts;
  }
}
