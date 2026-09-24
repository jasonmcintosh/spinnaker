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
import com.netflix.spinnaker.orca.retrofit.exceptions.SpinnakerServerExceptionHandler
import okhttp3.MediaType
import okhttp3.ResponseBody
import retrofit2.Response
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

class StartScriptTaskSpec extends Specification {

  @Subject
  StartScriptTask task = new StartScriptTask()

  void setup() {
    task.objectMapper = new ObjectMapper()
    task.spinnakerServerExceptionHandler = new SpinnakerServerExceptionHandler()
    task.master = "builds"
  }

  @Shared
  def pipeline = PipelineExecutionImpl.newPipeline("orca")

  def "parses queuedBuild and alreadyQueued from the new JSON response body"() {
    given:
    def stage = new StageExecutionImpl(pipeline, "script", [scriptPath: "/foo.sh", command: "run", job: "job1"])

    and:
    task.buildService = Stub(BuildService) {
      build(_, _, _, _) >>
          Response.success(200, ResponseBody.create(MediaType.parse("application/json"),
              new ObjectMapper().writeValueAsString([queuedBuild: "42", alreadyQueued: true])))
    }

    when:
    def result = task.execute(stage)

    then:
    result.status == ExecutionStatus.SUCCEEDED
    result.context.queuedBuild == "42"
    result.context.alreadyQueued == true
  }

  def "falls back to treating the response body as a plain queued build id from an old igor"() {
    given:
    def stage = new StageExecutionImpl(pipeline, "script", [scriptPath: "/foo.sh", command: "run", job: "job1"])

    and:
    task.buildService = Stub(BuildService) {
      build(_, _, _, _) >>
          Response.success(200, ResponseBody.create(MediaType.parse("text/plain"), "42"))
    }

    when:
    def result = task.execute(stage)

    then:
    result.status == ExecutionStatus.SUCCEEDED
    result.context.queuedBuild == "42"
    !result.context.containsKey("alreadyQueued")
  }
}
