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

package com.netflix.kayenta.gcs.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.kayenta.canary.CanaryConfig;
import com.netflix.kayenta.google.security.GoogleNamedAccountCredentials;
import com.netflix.kayenta.index.CanaryConfigIndex;
import com.netflix.kayenta.index.config.CanaryConfigIndexAction;
import com.netflix.kayenta.security.AccountCredentialsRepository;
import com.netflix.kayenta.storage.ObjectType;
import com.netflix.kayenta.storage.StorageService;
import com.netflix.spinnaker.kork.web.exceptions.NotFoundException;
import jakarta.validation.constraints.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

@Builder
@Slf4j
public class GcsStorageService implements StorageService {

  @Autowired
  private ObjectMapper kayentaObjectMapper;

  @NotNull
  @Singular
  @Getter
  private List<String> accountNames;

  @Autowired
  private AccountCredentialsRepository accountCredentialsRepository;

  @Autowired
  private CanaryConfigIndex canaryConfigIndex;

  @Override
  public boolean servicesAccount(String accountName) {
    return accountNames.contains(accountName);
  }

  /**
   * Check to see if the bucket exists, creating it if it is not there.
   */
  public void ensureBucketExists(String accountName) {
    GoogleNamedAccountCredentials credentials =
      accountCredentialsRepository.getRequiredOne(accountName);
    Storage storage = credentials.getStorage();
    String projectName = credentials.getProject();
    String bucketName = credentials.getBucket();
    String bucketLocation = credentials.getBucketLocation();

    Bucket bucket = storage.get(bucketName);
    if (!bucket.exists()) {
      log.info("Creating bucket {}", bucketName);
      BucketInfo.Builder bucketBuildRequest = BucketInfo
        .newBuilder(bucketName)
        .setVersioningEnabled(true);
      if (!StringUtils.isEmpty(bucketLocation)) {
        log.warn("Using location {} for bucket {}.", bucket, projectName);

        bucketBuildRequest.setLocation(bucketLocation);
      }
      try {
        storage.create(bucketBuildRequest.build());
      } catch (Exception e) {
        log.error("Could not create bucket {} in project {}: {}", bucketName, projectName, e);
        throw new IllegalArgumentException(e);
      }
    }

  }

  @Override
  public <T> T loadObject(String accountName, ObjectType objectType, String objectKey)
    throws IllegalArgumentException, NotFoundException {
    GoogleNamedAccountCredentials credentials =
      accountCredentialsRepository.getRequiredOne(accountName);
    Storage storage = credentials.getStorage();
    String bucketName = credentials.getBucket();
    Blob item;

    try {
      item = resolveSingularItem(objectType, objectKey, credentials, storage, bucketName);
    } catch (IllegalArgumentException e) {
      throw new NotFoundException(e.getMessage());
    }

    try {
      return deserialize(item, (TypeReference<T>) objectType.getTypeReference());
    } catch (IOException e) {
      if (e instanceof HttpResponseException) {
        HttpResponseException hre = (HttpResponseException) e;
        log.error(
          "Failed to load {} {}: {} {}",
          objectType.getGroup(),
          objectKey,
          hre.getStatusCode(),
          hre.getStatusMessage());
        if (hre.getStatusCode() == 404) {
          throw new NotFoundException("No file at path " + item.getName() + ".");
        }
      }
      throw new IllegalStateException(e);
    }
  }

  private Blob resolveSingularItem(
    ObjectType objectType,
    String objectKey,
    GoogleNamedAccountCredentials credentials,
    Storage storage,
    String bucketName) {
    String rootFolder = daoRoot(credentials, objectType.getGroup()) + "/" + objectKey;

    List<Blob> objectList = storage.get(bucketName).list(Storage.BlobListOption.prefix(rootFolder)).streamAll().toList();

    if (!objectList.isEmpty()) {
      return objectList.get(0);
    } else {
      throw new IllegalArgumentException(
        "Unable to resolve singular "
          + objectType
          + " at "
          + daoRoot(credentials, objectType.getGroup())
          + '/'
          + objectKey
          + ".");
    }
  }

  private <T> T deserialize(Blob object, TypeReference<T> typeReference)
    throws IOException {
    ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
    object.downloadTo(output);
    String json = output.toString("UTF8");

    return kayentaObjectMapper.readValue(json, typeReference);
  }

  @Override
  public <T> void storeObject(
    String accountName,
    ObjectType objectType,
    String objectKey,
    T obj,
    String filename,
    boolean isAnUpdate) {
    GoogleNamedAccountCredentials credentials =
      accountCredentialsRepository.getRequiredOne(accountName);
    Storage storage = credentials.getStorage();
    String bucketName = credentials.getBucket();
    String path = keyToPath(credentials, objectType, objectKey, filename);

    ensureBucketExists(accountName);

    long updatedTimestamp = -1;
    String correlationId = null;
    String canaryConfigSummaryJson = null;
    Blob originalItem = null;

    if (objectType == ObjectType.CANARY_CONFIG) {
      updatedTimestamp = canaryConfigIndex.getRedisTime();

      CanaryConfig canaryConfig = (CanaryConfig) obj;

      checkForDuplicateCanaryConfig(canaryConfig, objectKey, credentials);

      if (isAnUpdate) {
        // Storing a canary config while not checking for naming collisions can only be a PUT (i.e.
        // an update to an existing config).
        originalItem = resolveSingularItem(objectType, objectKey, credentials, storage, bucketName);
      }

      correlationId = UUID.randomUUID().toString();

      Map<String, Object> canaryConfigSummary =
        new ImmutableMap.Builder<String, Object>()
          .put("id", objectKey)
          .put("name", canaryConfig.getName())
          .put("updatedTimestamp", updatedTimestamp)
          .put("updatedTimestampIso", Instant.ofEpochMilli(updatedTimestamp).toString())
          .put("applications", canaryConfig.getApplications())
          .build();

      try {
        canaryConfigSummaryJson = kayentaObjectMapper.writeValueAsString(canaryConfigSummary);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(
          "Problem serializing canaryConfigSummary -> " + canaryConfigSummary, e);
      }

      canaryConfigIndex.startPendingUpdate(
        credentials,
        updatedTimestamp + "",
        CanaryConfigIndexAction.UPDATE,
        correlationId,
        canaryConfigSummaryJson);
    }

    try {
      byte[] bytes = kayentaObjectMapper.writeValueAsBytes(obj);
      storage.get(bucketName).create(path, bytes, "application/json");

      if (objectType == ObjectType.CANARY_CONFIG) {
        // This will be true if the canary config is renamed.
        if (originalItem != null && !originalItem.getName().equals(path)) {
          storage.get(bucketName).get(originalItem.getName()).delete();
        }

        canaryConfigIndex.finishPendingUpdate(
          credentials, CanaryConfigIndexAction.UPDATE, correlationId);
      }
    } catch (IOException e) {
      log.error("Update failed on path {}: {}", path, e);

      if (objectType == ObjectType.CANARY_CONFIG) {
        canaryConfigIndex.removeFailedPendingUpdate(
          credentials,
          updatedTimestamp + "",
          CanaryConfigIndexAction.UPDATE,
          correlationId,
          canaryConfigSummaryJson);
      }

      throw new IllegalArgumentException(e);
    }
  }

  private void checkForDuplicateCanaryConfig(
    CanaryConfig canaryConfig, String canaryConfigId, GoogleNamedAccountCredentials credentials) {
    String canaryConfigName = canaryConfig.getName();
    List<String> applications = canaryConfig.getApplications();
    String existingCanaryConfigId =
      canaryConfigIndex.getIdFromName(credentials, canaryConfigName, applications);

    // We want to avoid creating a naming collision due to the renaming of an existing canary
    // config.
    if (!StringUtils.isEmpty(existingCanaryConfigId)
      && !existingCanaryConfigId.equals(canaryConfigId)) {
      throw new IllegalArgumentException(
        "Canary config with name '"
          + canaryConfigName
          + "' already exists in the scope of applications "
          + applications
          + ".");
    }
  }

  @Override
  public void deleteObject(String accountName, ObjectType objectType, String objectKey) {
    GoogleNamedAccountCredentials credentials =
      accountCredentialsRepository.getRequiredOne(accountName);
    Storage storage = credentials.getStorage();
    String bucketName = credentials.getBucket();
    Blob item =
      resolveSingularItem(objectType, objectKey, credentials, storage, bucketName);

    long updatedTimestamp = -1;
    String correlationId = null;
    String canaryConfigSummaryJson = null;

    if (objectType == ObjectType.CANARY_CONFIG) {
      updatedTimestamp = canaryConfigIndex.getRedisTime();

      Map<String, Object> existingCanaryConfigSummary =
        canaryConfigIndex.getSummaryFromId(credentials, objectKey);

      if (existingCanaryConfigSummary != null) {
        String canaryConfigName = (String) existingCanaryConfigSummary.get("name");
        List<String> applications = (List<String>) existingCanaryConfigSummary.get("applications");

        correlationId = UUID.randomUUID().toString();

        Map<String, Object> canaryConfigSummary =
          new ImmutableMap.Builder<String, Object>()
            .put("id", objectKey)
            .put("name", canaryConfigName)
            .put("updatedTimestamp", updatedTimestamp)
            .put("updatedTimestampIso", Instant.ofEpochMilli(updatedTimestamp).toString())
            .put("applications", applications)
            .build();

        try {
          canaryConfigSummaryJson = kayentaObjectMapper.writeValueAsString(canaryConfigSummary);
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException(
            "Problem serializing canaryConfigSummary -> " + canaryConfigSummary, e);
        }

        canaryConfigIndex.startPendingUpdate(
          credentials,
          updatedTimestamp + "",
          CanaryConfigIndexAction.DELETE,
          correlationId,
          canaryConfigSummaryJson);
      }
    }

    try {
      Blob itemToDelete = storage.get(bucketName).get(item.getName());
      if (!itemToDelete.exists()) {
        if (correlationId != null) {
          canaryConfigIndex.finishPendingUpdate(
            credentials, CanaryConfigIndexAction.DELETE, correlationId);
        }
        return;
      }
      itemToDelete.delete();

      if (correlationId != null) {
        canaryConfigIndex.finishPendingUpdate(
          credentials, CanaryConfigIndexAction.DELETE, correlationId);
      }


    } catch (Exception ioex) {
      log.error("Failed to delete path {}: {}", item.getName(), ioex);

      if (correlationId != null) {
        canaryConfigIndex.removeFailedPendingUpdate(
          credentials,
          updatedTimestamp + "",
          CanaryConfigIndexAction.DELETE,
          correlationId,
          canaryConfigSummaryJson);
      }

      throw new IllegalArgumentException(ioex);
    }
  }

  @Override
  public List<Map<String, Object>> listObjectKeys(
    String accountName, ObjectType objectType, List<String> applications, boolean skipIndex) {
    GoogleNamedAccountCredentials credentials =
      accountCredentialsRepository.getRequiredOne(accountName);

    if (!skipIndex && objectType == ObjectType.CANARY_CONFIG) {
      Set<Map<String, Object>> canaryConfigSet =
        canaryConfigIndex.getCanaryConfigSummarySet(credentials, applications);

      return Lists.newArrayList(canaryConfigSet);
    } else {
      Storage storage = credentials.getStorage();
      String bucketName = credentials.getBucket();
      String rootFolder = daoRoot(credentials, objectType.getGroup());

      ensureBucketExists(accountName);

      int skipToOffset = rootFolder.length() + 1; // + Trailing slash
      List<Map<String, Object>> result = new ArrayList<>();

      log.debug("Listing {}", objectType.getGroup());

      try {
        List<Blob> objectList = storage.get(bucketName).list(Storage.BlobListOption.prefix(rootFolder)).streamAll().toList();

        for (Blob item : objectList) {
          String itemName = item.getName();
          int indexOfLastSlash = itemName.lastIndexOf("/");
          Map<String, Object> objectMetadataMap = new HashMap<>();
          long updatedTimestamp = item.getUpdateTimeOffsetDateTime().toEpochSecond();

          objectMetadataMap.put("id", itemName.substring(skipToOffset, indexOfLastSlash));
          objectMetadataMap.put("updatedTimestamp", updatedTimestamp);
          objectMetadataMap.put(
            "updatedTimestampIso", Instant.ofEpochMilli(updatedTimestamp).toString());

          if (objectType == ObjectType.CANARY_CONFIG) {
            String name = itemName.substring(indexOfLastSlash + 1);

            if (name.endsWith(".json")) {
              name = name.substring(0, name.length() - 5);
            }

            objectMetadataMap.put("name", name);
          }

          result.add(objectMetadataMap);
        }

      } catch (Exception e) {
        log.error("Could not fetch items from Google Cloud Storage: {}", e);
      }

      return result;
    }
  }

  private String daoRoot(GoogleNamedAccountCredentials credentials, String daoTypeName) {
    return credentials.getRootFolder() + '/' + daoTypeName;
  }

  private String keyToPath(
    GoogleNamedAccountCredentials credentials,
    ObjectType objectType,
    String objectKey,
    String filename) {
    if (filename == null) {
      filename = objectType.getDefaultFilename();
    }

    return daoRoot(credentials, objectType.getGroup()) + '/' + objectKey + '/' + filename;
  }
}
