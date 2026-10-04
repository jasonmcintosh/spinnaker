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
package com.netflix.spinnaker.config

import com.fasterxml.jackson.annotation.JsonTypeName
import com.netflix.spinnaker.clouddriver.event.AbstractSpinnakerEvent
import com.netflix.spinnaker.clouddriver.event.SpinnakerEvent
import com.netflix.spinnaker.kork.jackson.ObjectMapperSubtypeConfigurer
import com.netflix.spinnaker.kork.jackson.ObjectMapperSubtypeConfigurer.ClassSubtypeLocator
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import tools.jackson.databind.exc.InvalidTypeIdException
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule

/**
 * [SqlConfiguration.sqlEventRepository] must hand the repository the mapper that
 * [ObjectMapperSubtypeConfigurer] returns. Under Jackson 3 the configurer no longer mutates the
 * mapper it is given, so discarding its result leaves events unreadable.
 */
class SqlConfigurationEventMapperTest {

  private val locators = listOf(
    ClassSubtypeLocator(SpinnakerEvent::class.java, listOf("com.netflix.spinnaker.config"))
  )

  private fun baseMapper() = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()

  private val json = """{"eventType":"sqlConfigTestEvent","value":"v"}"""

  @Test
  fun `discarding the configurer result leaves the mapper unable to read events (the bug)`() {
    val mapper = baseMapper()

    // This is what sqlEventRepository did before: the returned mapper is thrown away.
    ObjectMapperSubtypeConfigurer(true).registerSubtypes(mapper, locators)

    expectThrows<InvalidTypeIdException> { mapper.readValue(json, SpinnakerEvent::class.java) }
  }

  @Test
  fun `withEventSubtypes returns a mapper that reads and writes registered events`() {
    val mapper = SqlConfiguration.withEventSubtypes(baseMapper(), locators)

    val event = mapper.readValue(json, SpinnakerEvent::class.java)

    expectThat(event).isA<SqlConfigTestEvent>().get { value }.isEqualTo("v")
    expectThat(mapper.writeValueAsString(event).contains("sqlConfigTestEvent")).isEqualTo(true)
  }
}

@JsonTypeName("sqlConfigTestEvent")
class SqlConfigTestEvent(val value: String) : AbstractSpinnakerEvent()
