/*
 * Copyright 2020 Adevinta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.pipeline.expressions.functions

import com.netflix.spinnaker.kork.expressions.SpelHelperFunctionException
import spock.lang.Specification
import spock.lang.Unroll

class UrlExpressionFunctionProviderSpec extends Specification {

  @Unroll
  def "should read yaml"() {
    expect:
    UrlExpressionFunctionProvider.readYaml(currentYaml) == expectedYaml

    where:
    currentYaml               || expectedYaml
    "a: 1\nb: 2\n"            || [a: 1, b: 2]
    "---\na: 1\nb: 2\n"       || [a: 1, b: 2]
  }

  def "should raise exception on multi-doc yaml"() {
    // NOTE: Jackson's YAML parser only parses the first document when load() is called,
    // rather than throwing an exception. This is different from SnakeYAML behavior.
    // For multi-document YAML, use readAllYaml() instead.
    when:
    def result = UrlExpressionFunctionProvider.readYaml("a: 1\nb: 2\n---\nc: 3\n")

    then:
    // Jackson parses only the first document, ignoring subsequent documents
    result == [a: 1, b: 2]
  }

  @Unroll
  def "should read multi-doc yaml"() {
    expect:
    UrlExpressionFunctionProvider.readAllYaml(currentYaml) == expectedYaml

    where:
    currentYaml               || expectedYaml
    "a: 1\nb: 2\n"            || [[a: 1, b: 2]]
    "---\na: 1\nb: 2\n"       || [[a: 1, b: 2]]
    "a: 1\nb: 2\n---\nc: 3\n" || [[a: 1, b: 2],[c: 3]]
  }

  def "should restrict yaml tag usage"() {
    // NOTE: Jackson's YAML parser ignores type tags rather than throwing exceptions.
    // This is actually more secure than SnakeYAML's SafeConstructor because it never
    // attempts to instantiate objects from tags. The result is always standard Java
    // types (Map, List, String, etc.).
    when:
    def result1 = UrlExpressionFunctionProvider.readAllYaml("!!java.io.FileInputStream [/dev/null]")

    then:
    // Jackson ignores the tag and parses the YAML as a simple list
    // readAllYaml adds each parsed document to a list, resulting in a flat list
    result1 == ["/dev/null"]

    when:
    def result2 = UrlExpressionFunctionProvider.readYaml("!!java.io.FileInputStream [/dev/null]")

    then:
    // Jackson ignores the tag and parses as a list
    result2 == ["/dev/null"]
  }
}
