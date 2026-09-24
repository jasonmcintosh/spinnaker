/*
 * Copyright 2026 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.igor.tasks

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper

class QueuedBuildResponseParser {
  /**
   * Igor's build-trigger endpoint returns a JSON object (e.g. {@code {"queuedBuild": "42",
   * "alreadyQueued": false}}) once it understands includeQueuedBuildMetadata=true. During a
   * rolling upgrade, an old igor instance won't recognize that query parameter and instead
   * returns the bare queued build id as plain text, so that shape must still be handled.
   */
  static Map<String, Object> parse(ObjectMapper objectMapper, String body) {
    try {
      Map<String, Object> parsed = objectMapper.readValue(body, Map.class)
      Map<String, Object> result = [queuedBuild: parsed.queuedBuild]
      if (parsed.containsKey('alreadyQueued')) {
        result.alreadyQueued = parsed.alreadyQueued
      }
      return result
    } catch (JsonProcessingException ignored) {
      return [queuedBuild: body]
    }
  }
}
