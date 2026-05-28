package com.netflix.spinnaker.kork.yaml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import lombok.extern.log4j.Log4j2;

/**
 * A wrapper around Jackson's YAMLMapper that provides a compatible API with SnakeYAML's Yaml class.
 *
 * <p>This class enables migration from SnakeYAML to Jackson YAML while maintaining backward
 * compatibility with existing code that uses the SnakeYAML API.
 *
 * <p>Security properties like code point limits and alias limits are enforced through Jackson's
 * stream constraints configuration.
 */
@Log4j2
public class JacksonYamlWrapper {

  private final ObjectMapper mapper;

  /**
   * Creates a new JacksonYamlWrapper with the specified ObjectMapper.
   *
   * @param mapper the Jackson ObjectMapper configured with YAMLFactory
   */
  public JacksonYamlWrapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /**
   * Parses a YAML string and returns the result as an Object.
   *
   * @param yaml the YAML content to parse
   * @return the parsed object (typically Map or List)
   */
  public Object load(String yaml) {
    try {
      return mapper.readValue(yaml, Object.class);
    } catch (JsonProcessingException e) {
      throw new YamlProcessingException("Failed to parse YAML", e);
    }
  }

  /**
   * Parses a YAML input stream and returns the result as an Object.
   *
   * @param io the input stream containing YAML content
   * @return the parsed object (typically Map or List)
   */
  public Object load(InputStream io) {
    try {
      return mapper.readValue(io, Object.class);
    } catch (IOException e) {
      throw new YamlProcessingException("Failed to parse YAML from InputStream", e);
    }
  }

  /**
   * Parses a YAML reader and returns the result as an Object.
   *
   * @param io the reader containing YAML content
   * @return the parsed object (typically Map or List)
   */
  public Object load(Reader io) {
    try {
      return mapper.readValue(io, Object.class);
    } catch (IOException e) {
      throw new YamlProcessingException("Failed to parse YAML from Reader", e);
    }
  }

  /**
   * Parses a YAML string and returns the result as an instance of the specified type.
   *
   * @param yaml the YAML content to parse
   * @param type the target class type
   * @param <T> the type parameter
   * @return the parsed object as the specified type
   */
  public <T> T loadAs(String yaml, Class<T> type) {
    try {
      return mapper.readValue(yaml, type);
    } catch (JsonProcessingException e) {
      throw new YamlProcessingException("Failed to parse YAML as " + type.getName(), e);
    }
  }

  /**
   * Parses a YAML input stream and returns the result as an instance of the specified type.
   *
   * @param io the input stream containing YAML content
   * @param type the target class type
   * @param <T> the type parameter
   * @return the parsed object as the specified type
   */
  public <T> T loadAs(InputStream io, Class<T> type) {
    try {
      return mapper.readValue(io, type);
    } catch (IOException e) {
      throw new YamlProcessingException("Failed to parse YAML as " + type.getName(), e);
    }
  }

  /**
   * Parses all YAML documents from a string and returns them as an Iterable.
   *
   * @param yaml the YAML content containing one or more documents
   * @return an iterable of parsed objects
   */
  public Iterable<Object> loadAll(String yaml) {
    try {
      return mapper.readerFor(Object.class).readValues(yaml).readAll();
    } catch (IOException e) {
      throw new YamlProcessingException("Failed to parse multiple YAML documents", e);
    }
  }

  /**
   * Parses all YAML documents from an input stream and returns them as an Iterable.
   *
   * @param io the input stream containing one or more YAML documents
   * @return an iterable of parsed objects
   */
  public Iterable<Object> loadAll(InputStream io) {
    try {
      return mapper.readerFor(Object.class).readValues(io).readAll();
    } catch (IOException e) {
      throw new YamlProcessingException("Failed to parse multiple YAML documents from stream", e);
    }
  }

  /**
   * Serializes a Java object to YAML format.
   *
   * @param data the object to serialize
   * @return the YAML string representation
   */
  public String dump(Object data) {
    try {
      return mapper.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new YamlProcessingException("Failed to serialize object to YAML", e);
    }
  }

  /**
   * Serializes a Java object to YAML format and writes it to the specified writer.
   *
   * @param data the object to serialize
   * @param output the writer to write to
   */
  public void dump(Object data, Writer output) {
    try {
      mapper.writeValue(output, data);
    } catch (IOException e) {
      throw new YamlProcessingException("Failed to write YAML to Writer", e);
    }
  }

  /**
   * Serializes multiple Java objects to YAML format as multiple documents.
   *
   * @param data the iterable of objects to serialize
   * @return the YAML string representation with document separators
   */
  public String dumpAll(Iterable<?> data) {
    try {
      StringWriter writer = new StringWriter();
      boolean first = true;
      for (Object item : data) {
        if (!first) {
          writer.write("---\n");
        }
        writer.write(mapper.writeValueAsString(item));
        first = false;
      }
      return writer.toString();
    } catch (IOException e) {
      throw new YamlProcessingException("Failed to serialize multiple objects to YAML", e);
    }
  }

  /**
   * Gets the underlying ObjectMapper for advanced configuration.
   *
   * @return the Jackson ObjectMapper
   */
  public ObjectMapper getMapper() {
    return mapper;
  }

  /** Exception thrown when YAML processing fails. */
  public static class YamlProcessingException extends RuntimeException {
    public YamlProcessingException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
