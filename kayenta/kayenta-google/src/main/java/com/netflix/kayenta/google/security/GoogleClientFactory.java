/*
 * Copyright 2017 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

package com.netflix.kayenta.google.security;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.Optional;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ToString
@Slf4j
public class GoogleClientFactory {

  private static String applicationVersion =
      Optional.ofNullable(GoogleClientFactory.class.getPackage().getImplementationVersion())
          .orElse("Unknown");

  @Getter private String project;

  public GoogleClientFactory(String project) {
    this.project = project;
  }

  public MetricServiceClient getMonitoring() throws IOException {
    return MetricServiceClient.create(
        MetricServiceSettings.newBuilder().setCredentialsProvider(this::getCredentials).build());
  }

  public Storage getStorage() throws IOException {
    return StorageOptions.newBuilder()
        .setProjectId(project)
        .setCredentials(getCredentials())
        .setTransportOptions(
            HttpTransportOptions.newBuilder()
                .setReadTimeout(2 * 60000)
                .setConnectTimeout(5000)
                .build())
        .build()
        .getService();
  }

  protected Credentials getCredentials() throws IOException {
    return GoogleCredentials.getApplicationDefault();
  }
}
