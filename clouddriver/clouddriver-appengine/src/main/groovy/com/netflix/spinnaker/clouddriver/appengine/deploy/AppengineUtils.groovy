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

package com.netflix.spinnaker.clouddriver.appengine.deploy

import com.google.appengine.v1.ListServicesRequest
import com.google.appengine.v1.ListVersionsRequest
import com.google.appengine.v1.Service
import com.google.appengine.v1.ServicesClient
import com.google.appengine.v1.Version
import com.google.appengine.v1.VersionsClient
import com.netflix.spinnaker.clouddriver.appengine.security.AppengineNamedAccountCredentials
import com.netflix.spinnaker.clouddriver.data.task.Task

class AppengineUtils {
  static List<Version> queryAllVersions(String project,
                                        AppengineNamedAccountCredentials credentials,
                                        Task task,
                                        String phase) {
    task.updateStatus phase, "Querying all versions for project $project..."
    def services = queryAllServices(project, credentials, task, phase)

    def allVersions = []
    VersionsClient versionsClient = credentials.credentials.getVersionsClient()
    try {
      services.each { service ->
        def listVersionsRequest = ListVersionsRequest.newBuilder()
            .setParent("apps/${project}/services/${service.getId()}")
            .build()
        def versions = versionsClient.listVersions(listVersionsRequest).iterateAll().toList()
        if (versions) {
          allVersions.addAll(versions)
        }
      }
    } finally {
      versionsClient.close()
    }

    return allVersions
  }

  static List<Service> queryAllServices(String project,
                                        AppengineNamedAccountCredentials credentials,
                                        Task task,
                                        String phase) {
    task.updateStatus phase, "Querying services for project $project..."
    ServicesClient servicesClient = credentials.credentials.getServicesClient()
    try {
      def listServicesRequest = ListServicesRequest.newBuilder()
          .setParent("apps/${project}")
          .build()
      return servicesClient.listServices(listServicesRequest).iterateAll().toList()
    } finally {
      servicesClient.close()
    }
  }

  static List<Version> queryVersionsForService(String project,
                                               String service,
                                               AppengineNamedAccountCredentials credentials,
                                               Task task,
                                               String phase) {
    task.updateStatus phase, "Querying versions for project $project and service $service"
    VersionsClient versionsClient = credentials.credentials.getVersionsClient()
    try {
      def listVersionsRequest = ListVersionsRequest.newBuilder()
          .setParent("apps/${project}/services/${service}")
          .build()
      return versionsClient.listVersions(listVersionsRequest).iterateAll().toList()
    } finally {
      versionsClient.close()
    }
  }

  static Service queryService(String project,
                              String service,
                              AppengineNamedAccountCredentials credentials,
                              Task task,
                              String phase) {
    task.updateStatus phase, "Querying service $service for project $project..."
    ServicesClient servicesClient = credentials.credentials.getServicesClient()
    try {
      def serviceName = "apps/${project}/services/${service}"
      return servicesClient.getService(serviceName)
    } finally {
      servicesClient.close()
    }
  }
}
