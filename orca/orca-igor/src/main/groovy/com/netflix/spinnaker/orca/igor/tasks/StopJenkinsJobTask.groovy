/*
 * Copyright 2015 Netflix, Inc.
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
import com.netflix.spinnaker.orca.api.pipeline.Task
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution
import com.netflix.spinnaker.orca.api.pipeline.TaskResult
import com.netflix.spinnaker.orca.igor.BuildService
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import javax.annotation.Nonnull

@Slf4j
@Component
class StopJenkinsJobTask implements Task {

  @Autowired
  BuildService buildService

  @Autowired
  ObjectMapper objectMapper

  @Nonnull
  @Override
  TaskResult execute(@Nonnull StageExecution stage) {
    String master = stage.context.master
    String job = stage.context.job
    String queuedBuild = stage.context.queuedBuild
    Integer buildNumber = stage.context.buildNumber ? (Integer) stage.context.buildNumber : 0
    boolean alreadyQueued = stage.context.alreadyQueued as boolean

    if (alreadyQueued) {
      // Igor reported that this build was already queued/running under a non-concurrent job
      // before this stage started it, so this stage doesn't own it and must not cancel it.
      log.info("Skipping stop of job={} on master={} because it was already queued/running before this stage started it", job, master)
      return TaskResult.SUCCEEDED
    }

    if (queuedBuild != null) {
      buildService.stop(master, job, queuedBuild, buildNumber)
    }

    TaskResult.SUCCEEDED
  }
}
