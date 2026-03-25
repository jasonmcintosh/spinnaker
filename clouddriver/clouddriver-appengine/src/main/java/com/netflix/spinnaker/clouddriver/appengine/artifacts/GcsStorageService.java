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

package com.netflix.spinnaker.clouddriver.appengine.artifacts;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsStorageService {
  private static final Logger log = LoggerFactory.getLogger(GcsStorageService.class);

  public static class Factory {
    public GcsStorageService newForCredentials(String credentialsPath) throws IOException {
      GoogleCredentials credentials;
      if (credentialsPath != null && !credentialsPath.isEmpty()) {
        FileInputStream stream = new FileInputStream(credentialsPath);
        credentials =
            GoogleCredentials.fromStream(stream)
                .createScoped(
                    Collections.singleton("https://www.googleapis.com/auth/devstorage.read_only"));
        log.info("Loaded credentials from " + credentialsPath);
      } else {
        log.info(
            "spinnaker.gcs.enabled without spinnaker.gcs.jsonPath. "
                + "Using default application credentials. Using default credentials.");
        credentials = GoogleCredentials.getApplicationDefault();
      }
      return new GcsStorageService(
          StorageOptions.newBuilder().setCredentials(credentials).build().getService());
    }
  }

  private Storage storage;

  public GcsStorageService(Storage storage) {
    this.storage = storage;
  }

  public ReadChannel openObjectStream(String bucketName, String path, Long generation) {
    Blob blob = null;
    if (generation != null) {
      storage.get(bucketName).get(path, Storage.BlobGetOption.generationMatch(generation));
    } else {
      blob = storage.get(bucketName).get(path);
    }
    return blob.reader(Blob.BlobSourceOption.metagenerationMatch());
  }

  public interface VisitorOperation {
    void visit(Blob storageObj) throws IOException;
  }

  public void visitObjects(String bucketName, String pathPrefix, VisitorOperation visitor) {

    Iterable<Blob> objects =
        storage.get(bucketName).list(Storage.BlobListOption.matchGlob(pathPrefix)).iterateAll();
    ExecutorService executor =
        Executors.newFixedThreadPool(
            8,
            new ThreadFactoryBuilder()
                .setNameFormat(GcsStorageService.class.getSimpleName() + "-%d")
                .build());

    for (Blob obj : objects) {
      executor.submit(
          () -> {
            try {
              visitor.visit(obj);
            } catch (IOException ioex) {
              throw new IllegalStateException(ioex);
            }
          });
    }
    executor.shutdown();

    try {
      if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
        throw new IllegalStateException("Timed out waiting to process StorageObjects.");
      }
    } catch (InterruptedException intex) {
      throw new IllegalStateException(intex);
    }
  }

  public void downloadStorageObjectRelative(Blob obj, String ignorePrefix, String baseDirectory)
      throws IOException {
    String objPath = obj.getName();
    if (!ignorePrefix.isEmpty()) {
      ignorePrefix += File.separator;
      if (!objPath.startsWith(ignorePrefix)) {
        throw new IllegalArgumentException(objPath + " does not start with '" + ignorePrefix + "'");
      }
      objPath = objPath.substring(ignorePrefix.length());
    }

    // Ignore folder placeholder objects created by Google Console UI
    if (objPath.endsWith("/")) {
      return;
    }
    File target = new File(baseDirectory, objPath);
    try (ReadChannel stream =
        openObjectStream(obj.getBucket(), obj.getName(), obj.getGeneration())) {
      ArtifactUtils.writeStreamToFile(stream, target);
    }
    target.setLastModified(obj.getUpdateTimeOffsetDateTime().toEpochSecond());
  }

  public void downloadStorageObject(Blob obj, String baseDirectory) throws IOException {
    downloadStorageObjectRelative(obj, "", baseDirectory);
  }
}
