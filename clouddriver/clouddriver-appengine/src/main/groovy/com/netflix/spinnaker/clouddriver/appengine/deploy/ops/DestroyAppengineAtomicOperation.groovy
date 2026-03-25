/*
 * Copyright 2016 Google, Inc.
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

import com.google.appengine.v1.VersionsClient
import com.netflix.spinnaker.clouddriver.appengine.deploy.AppengineSafeRetry
import com.netflix.spinnaker.clouddriver.appengine.deploy.description.DestroyAppengineDescription
import com.netflix.spinnaker.clouddriver.appengine.deploy.exception.AppengineResourceNotFoundException
import com.netflix.spinnaker.clouddriver.appengine.provider.view.AppengineClusterProvider
import com.netflix.spinnaker.clouddriver.data.task.Task
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired

@Slf4j
class DestroyAppengineAtomicOperation extends AppengineAtomicOperation<Void> {
  private static final String BASE_PHASE = "DESTROY_SERVER_GROUP"

  private static Task getTask() {
    TaskRepository.threadLocalTask.get()
  }

  private final DestroyAppengineDescription description

  @Autowired
  AppengineClusterProvider appengineClusterProvider

  @Autowired
  AppengineSafeRetry safeRetry

  DestroyAppengineAtomicOperation(DestroyAppengineDescription description) {
    this.description = description
  }
  /**
  * curl -X POST -H "Content-Type: application/json" -d '[ { "destroyServerGroup": { "serverGroupName": "app-stack-detail-v000", "credentials": "my-appengine-account" }} ]' localhost:7002/appengine/ops
  */
  @Override
  Void operate(List priorOutputs) {
    task.updateStatus BASE_PHASE, "Initializing destruction of server group $description.serverGroupName..."

    def credentials = description.credentials
    def project = credentials.project
    def serverGroupName = description.serverGroupName

    task.updateStatus BASE_PHASE, "Looking up $description.serverGroupName..."
    def serverGroup = appengineClusterProvider.getServerGroup(credentials.name,
                                                              credentials.region,
                                                              serverGroupName)
    def loadBalancerName = serverGroup?.loadBalancers?.first()

    if (!serverGroup || !loadBalancerName) {
      throw new AppengineResourceNotFoundException("Unable to locate server group $serverGroupName")
    }

    safeRetry.doRetry(
      {
        VersionsClient versionsClient = credentials.credentials.getVersionsClient()
        try {
          def versionName = "apps/${project}/services/${loadBalancerName}/versions/${serverGroupName}"
          versionsClient.deleteVersionAsync(versionName).get()
        } finally {
          versionsClient.close()
        }
      },
      "version",
      task,
      [409],
      [action: "Destroy", phase: BASE_PHASE],
      registry
    )

    task.updateStatus BASE_PHASE, "Successfully destroyed server group $serverGroupName."
    return null
  }
}
