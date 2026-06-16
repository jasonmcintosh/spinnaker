package com.netflix.spinnaker.keel.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.keel.exceptions.YamlParsingException
import com.netflix.spinnaker.kork.yaml.YamlHelper
import org.yaml.snakeyaml.LoaderOptions
import java.io.InputStream

/**
 * Reads [yaml] into an object of type [T] while allowing for anchors and aliases in the YAML.
 * This has to be done as a 2-step process because Jackson's YAML parser does not properly resolve
 * YAML anchors and aliases. First the input is resolved using SnakeYAML directly (which properly
 * handles anchors, aliases, and merge keys), then the resolved data structure is passed to the
 * YAMLMapper for conversion to the target type with Kotlin support.
 */
inline fun <reified T> YAMLMapper.readValueInliningAliases(yaml: String): T {
  try {
    val options = LoaderOptions()
    options.maxAliasesForCollections = 1000
    // Use SnakeYAML directly to properly resolve anchors, aliases, and merge keys
    val snakeYaml = org.yaml.snakeyaml.Yaml(org.yaml.snakeyaml.constructor.SafeConstructor(options))
    val resolvedData = snakeYaml.load(yaml)
    // Now use YAMLMapper's convertValue to deserialize to Kotlin class with proper Kotlin module support
    return convertValue(resolvedData)
  } catch (ex: Exception) {
    throw YamlParsingException(ex)
  }
}

/**
 * Converts a YAML stream into JSON with any anchors and aliases resolved.
 */
fun ObjectMapper.writeYamlAsJsonString(stream: InputStream): String =
  writeValueAsString(YamlHelper.newYamlSafeConstructor().load(stream))
