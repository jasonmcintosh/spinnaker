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

package com.netflix.spinnaker.clouddriver.appengine.security;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.appengine.v1.ApplicationsClient;
import com.google.appengine.v1.ApplicationsSettings;
import com.google.appengine.v1.InstancesClient;
import com.google.appengine.v1.InstancesSettings;
import com.google.appengine.v1.ServicesClient;
import com.google.appengine.v1.ServicesSettings;
import com.google.appengine.v1.VersionsClient;
import com.google.appengine.v1.VersionsSettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.netflix.spinnaker.clouddriver.googlecommon.security.GoogleCommonCredentials;
import java.io.IOException;

public class AppengineCredentials extends GoogleCommonCredentials {

  private final String project;

  public AppengineCredentials(String project) {
    this.project = project;
  }

  public ServicesClient getServicesClient() throws IOException {
    GoogleCredentials credentials = getCredentials();
    ServicesSettings settings =
        ServicesSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
    return ServicesClient.create(settings);
  }

  public VersionsClient getVersionsClient() throws IOException {
    GoogleCredentials credentials = getCredentials();
    VersionsSettings settings =
        VersionsSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
    return VersionsClient.create(settings);
  }

  public InstancesClient getInstancesClient() throws IOException {
    GoogleCredentials credentials = getCredentials();
    InstancesSettings settings =
        InstancesSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
    return InstancesClient.create(settings);
  }

  public ApplicationsClient getApplicationsClient() throws IOException {
    GoogleCredentials credentials = getCredentials();
    ApplicationsSettings settings =
        ApplicationsSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build();
    return ApplicationsClient.create(settings);
  }
}
