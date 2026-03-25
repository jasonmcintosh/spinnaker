/*
 * Copyright 2017 Google, Inc.
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

package com.netflix.spinnaker.clouddriver.appengine.deploy.ops

import com.google.appengine.v1.AutomaticScaling
import com.google.appengine.v1.UpdateVersionRequest
import com.google.appengine.v1.Version
import com.google.appengine.v1.VersionsClient
import com.google.longrunning.Operation
import com.google.protobuf.FieldMask
import com.netflix.spinnaker.clouddriver.appengine.deploy.AppengineSafeRetry
import com.netflix.spinnaker.clouddriver.appengine.deploy.description.UpsertAppengineAutoscalingPolicyDescription
import com.netflix.spinnaker.clouddriver.appengine.deploy.exception.AppengineResourceNotFoundException
import com.netflix.spinnaker.clouddriver.appengine.provider.view.AppengineClusterProvider
import com.netflix.spinnaker.clouddriver.data.task.Task
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository
import org.springframework.beans.factory.annotation.Autowired

class UpsertAppengineAutoscalingPolicyAtomicOperation extends AppengineAtomicOperation<Void> {
 private static final String BASE_PHASE = "UPSERT_SCALING_POLICY"

  private static Task getTask() {
    TaskRepository.threadLocalTask.get()
  }

  private final UpsertAppengineAutoscalingPolicyDescription description

  @Autowired
  AppengineClusterProvider appengineClusterProvider

  @Autowired
  AppengineSafeRetry safeRetry

  UpsertAppengineAutoscalingPolicyAtomicOperation(UpsertAppengineAutoscalingPolicyDescription description) {
    this.description = description
  }

  @Override
  Void operate(List priorOutputs) {
    task.updateStatus BASE_PHASE, "Initializing upsert of autoscaling policy for server group " +
      "$description.serverGroupName in $description.credentials.region..."

    def credentials = description.credentials
    def serverGroupName = description.serverGroupName
    def projectName = credentials.project

    task.updateStatus BASE_PHASE, "Looking up $description.serverGroupName..."
    def serverGroup = appengineClusterProvider.getServerGroup(credentials.name,
                                                              credentials.region,
                                                              serverGroupName)
    def loadBalancerName = serverGroup?.loadBalancers?.first()

    if (!serverGroup) {
      throw new AppengineResourceNotFoundException("Unable to locate server group $serverGroupName.")
    }
    if (!loadBalancerName) {
      throw new AppengineResourceNotFoundException("Unable to locate load balancer for $serverGroupName.")
    }

    def updatedAutoscalingPolicy = AutomaticScaling.newBuilder()
    if (description.minIdleInstances != null) {
      updatedAutoscalingPolicy.setMinIdleInstances(description.minIdleInstances)
    } else if (serverGroup.scalingPolicy?.minIdleInstances != null) {
      updatedAutoscalingPolicy.setMinIdleInstances(serverGroup.scalingPolicy.minIdleInstances)
    }
    if (description.maxIdleInstances != null) {
      updatedAutoscalingPolicy.setMaxIdleInstances(description.maxIdleInstances)
    } else if (serverGroup.scalingPolicy?.maxIdleInstances != null) {
      updatedAutoscalingPolicy.setMaxIdleInstances(serverGroup.scalingPolicy.maxIdleInstances)
    }

    def version = Version.newBuilder()
        .setName("apps/${projectName}/services/${loadBalancerName}/versions/${serverGroupName}")
        .setAutomaticScaling(updatedAutoscalingPolicy.build())
        .build()

    task.updateStatus BASE_PHASE, "Setting min and max idle instance boundaries for $serverGroupName..."
    safeRetry.doRetry(
      { callApi(projectName, loadBalancerName, serverGroupName, version) },
      "version",
      task,
      [409],
      [action: "upsertAutoscalingPolicy", phase: BASE_PHASE],
      registry)

    task.updateStatus BASE_PHASE, "Completed upsert of autoscaling policy for $serverGroupName."
    return null
  }

  Operation callApi(String projectName, String loadBalancerName, String serverGroupName, Version version) {
    VersionsClient versionsClient = description.credentials.credentials.getVersionsClient()
    try {
      def updateMask = FieldMask.newBuilder()
          .addPaths("automaticScaling.min_idle_instances")
          .addPaths("automaticScaling.max_idle_instances")
          .build()
      def updateRequest = UpdateVersionRequest.newBuilder()
          .setName("apps/${projectName}/services/${loadBalancerName}/versions/${serverGroupName}")
          .setVersion(version)
          .setUpdateMask(updateMask)
          .build()
      return versionsClient.updateVersionAsync(updateRequest).get()
    } finally {
      versionsClient.close()
    }
  }
}
