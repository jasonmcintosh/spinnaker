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

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus
import com.netflix.spinnaker.orca.igor.BuildService
import com.netflix.spinnaker.orca.pipeline.model.PipelineExecutionImpl
import com.netflix.spinnaker.orca.pipeline.model.StageExecutionImpl
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

class StopJenkinsJobTaskSpec extends Specification {

  @Subject
  StopJenkinsJobTask task = new StopJenkinsJobTask()

  BuildService buildService = Mock(BuildService)

  void setup() {
    task.objectMapper = new ObjectMapper()
    task.buildService = buildService
  }

  @Shared
  def pipeline = PipelineExecutionImpl.newPipeline("orca")

  def "stops the build when it was started by this stage"() {
    given:
    def stage = new StageExecutionImpl(pipeline, "jenkins",
        [master: "builds", job: "orca", queuedBuild: "42", buildNumber: 7])

    when:
    def result = task.execute(stage)

    then:
    1 * buildService.stop("builds", "orca", "42", 7)
    result.status == ExecutionStatus.SUCCEEDED
  }

  def "does not stop the build when igor reports it was already queued/running before this stage started it"() {
    given:
    def stage = new StageExecutionImpl(pipeline, "jenkins",
        [master: "builds", job: "orca", queuedBuild: "42", buildNumber: 7, alreadyQueued: true])

    when:
    def result = task.execute(stage)

    then:
    0 * buildService.stop(_, _, _, _)
    result.status == ExecutionStatus.SUCCEEDED
  }
}
