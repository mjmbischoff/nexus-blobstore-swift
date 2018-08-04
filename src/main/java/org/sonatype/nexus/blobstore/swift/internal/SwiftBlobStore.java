/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2017-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.blobstore.swift.internal;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.HashCode;
import org.apache.commons.io.FileUtils;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Directory;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.nexus.blobstore.BlobIdLocationResolver;
import org.sonatype.nexus.blobstore.BlobSupport;
import org.sonatype.nexus.blobstore.MetricsInputStream;
import org.sonatype.nexus.blobstore.StreamMetrics;
import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.blobstore.api.BlobAttributes;
import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.blobstore.api.BlobMetrics;
import org.sonatype.nexus.blobstore.api.BlobStore;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.blobstore.api.BlobStoreException;
import org.sonatype.nexus.blobstore.api.BlobStoreMetrics;
import org.sonatype.nexus.blobstore.api.BlobStoreUsageChecker;
import org.sonatype.nexus.common.stateguard.Guarded;
import org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.from;
import static java.lang.String.format;
import static org.sonatype.nexus.blobstore.DirectPathLocationStrategy.DIRECT_PATH_ROOT;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.FAILED;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.NEW;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STARTED;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STOPPED;

/**
 * A {@link BlobStore} that stores its content in Openstack SWIFT
 */
@Named(SwiftBlobStore.TYPE)
public class SwiftBlobStore extends StateGuardLifecycleSupport implements BlobStore {

  private final Logger timerlog = LoggerFactory.getLogger(SwiftBlobStore.class.getName()+"-timer");

  public static final String TYPE = "SWIFT";
  public static final String BLOB_CONTENT_SUFFIX = ".bytes";
  public static final String BLOB_ATTRIBUTE_SUFFIX = ".properties";
  public static final String CONFIG_KEY = "swift";
  public static final String CONTAINER_KEY = "container";
  public static final String SOCKET_TIMEOUT_KEY = "socketTimeout";
  public static final String USERNAME_KEY = "username";
  public static final String PASSWORD_KEY = "password";
  public static final String AUTHURL_KEY = "authUrl";
  public static final String AUTH_METHOD = "authMethod";
  public static final String TENANT_ID_KEY = "tenantId";
  public static final String TENANT_NAME_KEY = "tenantName";
  public static final String TRIES_KEY = "tries";
  public static final String METADATA_FILENAME = "metadata.properties";
  public static final String TYPE_KEY = "type";
  public static final String TYPE_V1 = "swift/1";
  public static final String CONTENT_PREFIX = "content";
  public static final String TEMPORARY_BLOB_ID_PREFIX = "tmp$";

  private static final Directory CONTENT_DIRECTORY = new Directory(CONTENT_PREFIX, '/');
  private static final int BUFFER_SIZE = 1024 * 1024 * 25;

  private final SwiftClientFactory swiftClientFactory;
  private final BlobIdLocationResolver blobIdLocationResolver;
  private final AtomicInteger tries = new AtomicInteger(1);

  private BlobStoreConfiguration blobStoreConfiguration;
  private SwiftBlobStoreMetricsStore storeMetrics;
  private LoadingCache<BlobId, SwiftBlob> liveBlobs;
  private Account swift;

  @Inject
  public SwiftBlobStore(final SwiftClientFactory swiftClientFactory,
                        final BlobIdLocationResolver blobIdLocationResolver,
                        final SwiftBlobStoreMetricsStore storeMetrics)
  {
    this.swiftClientFactory = checkNotNull(swiftClientFactory);
    this.blobIdLocationResolver = checkNotNull(blobIdLocationResolver);
    this.storeMetrics = checkNotNull(storeMetrics);
  }

  @Override
  protected void doStart() throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      // ensure blobstore is supported
      SwiftPropertiesFile metadata = new SwiftPropertiesFile(swift, getConfiguredContainer(), null, METADATA_FILENAME);
      if (metadata.exists()) {
        metadata.load();
        String type = metadata.getProperty(TYPE_KEY);
        checkState(TYPE_V1.equals(type), "Unsupported blob store type/version: %s in %s", type, metadata);
      } else {
        // assumes new blobstore, write out type
        metadata.setProperty(TYPE_KEY, TYPE_V1);
        metadata.store();
      }
      liveBlobs = CacheBuilder.newBuilder().weakValues().build(from(SwiftBlob::new));
      storeMetrics.setContainer(getConfiguredContainer());
      storeMetrics.setSwift(swift);
      storeMetrics.start();
    } finally {
      timerlog.debug("doStart() took: " + stopwatch);
    }
  }

  @Override
  protected void doStop() throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      liveBlobs = null;
      storeMetrics.stop();
    } finally {
      timerlog.debug("doStop() took: " + stopwatch);
    }
  }

  /**
   * Returns path for blob-id content file relative to root directory.
   */
  private String contentPath(final BlobId id) {
    return getLocation(id) + BLOB_CONTENT_SUFFIX;
  }

  /**
   * Returns path for blob-id attribute file relative to root directory.
   */
  private String attributePath(final BlobId id) {
    return getLocation(id) + BLOB_ATTRIBUTE_SUFFIX;
  }

  /**
   * Returns the location for a blob ID based on whether or not the blob ID is for a temporary or permanent blob.
   */
  private String getLocation(final BlobId id) {
    return CONTENT_PREFIX + "/" + blobIdLocationResolver.getLocation(id);
  }

  @Override
  @Guarded(by = STARTED)
  public  Blob create(final InputStream blobData, final Map<String, String> headers) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      checkNotNull(blobData);

      return create(headers, destination -> {
        File tempFile = File.createTempFile("nexus", "inflight");
        tempFile.deleteOnExit();
        try (MetricsInputStream input = new MetricsInputStream(blobData); Closeable ignored = () -> tempFile.delete()) {
          FileUtils.copyInputStreamToFile(input, tempFile);
          autoRetry(() -> {
            swift.getContainer(getConfiguredContainer()).getObject(destination).uploadObject(tempFile);
          });
          return input.getMetrics();
        } catch (Exception e) {
          throw new BlobStoreException("error uploading blob", e, null);
        }
      });
    } finally {
      timerlog.debug("create(...) took: " + stopwatch);
    }
  }

  @Override
  @Guarded(by = STARTED)
  public Blob create(final Path sourceFile, final Map<String, String> headers, final long size, final HashCode sha1) {
    throw new BlobStoreException("hard links not supported", null);
  }

  private Blob create(final Map<String, String> headers, final BlobIngester ingester) {
    checkNotNull(headers);

    checkArgument(headers.containsKey(BLOB_NAME_HEADER), "Missing header: %s", BLOB_NAME_HEADER);
    checkArgument(headers.containsKey(CREATED_BY_HEADER), "Missing header: %s", CREATED_BY_HEADER);

    final BlobId blobId = blobIdLocationResolver.fromHeaders(headers);

    final String blobPath = contentPath(blobId);
    final String attributePath = attributePath(blobId);
    final SwiftBlob blob = liveBlobs.getUnchecked(blobId);

    Lock lock = blob.lock();
    try {
      log.debug("Writing blob {} to {}", blobId, blobPath);

      final StreamMetrics streamMetrics = ingester.ingestTo(blobPath);
      final BlobMetrics metrics = new BlobMetrics(new DateTime(), streamMetrics.getSha1(), streamMetrics.getSize());
      blob.refresh(headers, metrics);

      SwiftBlobAttributes blobAttributes = new SwiftBlobAttributes(swift, getConfiguredContainer(), attributePath, headers, metrics);
      autoRetry(() -> blobAttributes.store());
      storeMetrics.recordAddition(blobAttributes.getMetrics().getContentSize());

      return blob;
    } catch (IOException e) {
      // Something went wrong, clean up the files we created
      deleteQuietly(attributePath);
      deleteQuietly(blobPath);
      throw new BlobStoreException(e, blobId);
    } finally {
      lock.unlock();
    }
  }

  @Override
  @Guarded(by = STARTED)
  public Blob copy(final BlobId blobId, final Map<String, String> headers) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      Blob sourceBlob = checkNotNull(get(blobId));
      String sourcePath = contentPath(sourceBlob.getId());
      return create(headers, destination -> this.<StreamMetrics, IOException>autoRetry(() -> {
        try (InputStream source = new BufferedInputStream(swift.getContainer(getConfiguredContainer()).getObject(sourcePath).downloadObjectAsInputStream(), BUFFER_SIZE)) {
          swift.getContainer(getConfiguredContainer()).getObject(destination).uploadObject(source);
          BlobMetrics metrics = sourceBlob.getMetrics();
          return new StreamMetrics(metrics.getContentSize(), metrics.getSha1Hash());
        }
      }));
    } finally {
      timerlog.debug("copy() took: " + stopwatch);
    }
  }

  @Nullable
  @Override
  @Guarded(by = STARTED)
  public Blob get(final BlobId blobId) {
    return get(blobId, false);
  }

  @Nullable
  @Override
  public Blob get(final BlobId blobId, final boolean includeDeleted) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      checkNotNull(blobId);

      final SwiftBlob blob = liveBlobs.getUnchecked(blobId);

      if (blob.isStale()) {
        Lock lock = blob.lock();
        try {
          if (blob.isStale()) {
            SwiftBlobAttributes blobAttributes = new SwiftBlobAttributes(swift, getConfiguredContainer(), attributePath(blobId));
            boolean loaded = blobAttributes.load();
            if (!loaded) {
              log.warn("Attempt to access non-existent blob {} ({})", blobId, blobAttributes);
              return null;
            }

            if (blobAttributes.isDeleted() && !includeDeleted) {
              log.warn("Attempt to access soft-deleted blob {} ({})", blobId, blobAttributes);
              return null;
            }

            blob.refresh(blobAttributes.getHeaders(), blobAttributes.getMetrics());
          }
        } catch (IOException e) {
          throw new BlobStoreException(e, blobId);
        }
        finally {
          lock.unlock();
        }
      }

      log.debug("Accessing blob {}", blobId);

      return blob;
    } finally {
      timerlog.trace("get(...) took: " + stopwatch);
    }
  }

  @Override
  @Guarded(by = STARTED)
  public boolean delete(final BlobId blobId, String reason) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      checkNotNull(blobId);

      final SwiftBlob blob = liveBlobs.getUnchecked(blobId);

      Lock lock = blob.lock();
      try {
        log.debug("Soft deleting blob {}", blobId);

        SwiftBlobAttributes blobAttributes = new SwiftBlobAttributes(swift, getConfiguredContainer(), attributePath(blobId).toString());
        boolean loaded = autoRetry(() -> blobAttributes.load());
        if (!loaded) {
          // This could happen under some concurrent situations (two threads try to delete the same blob)
          // but it can also occur if the deleted index refers to a manually-deleted blob.
          log.warn("Attempt to mark-for-delete non-existent blob {}", blobId);
          return false;
        }
        else if (blobAttributes.isDeleted()) {
          log.debug("Attempt to delete already-deleted blob {}", blobId);
          return false;
        }

        blobAttributes.setDeleted(true);
        blobAttributes.setDeletedReason(reason);
        autoRetry(blobAttributes::store);
        delete(contentPath(blobId));
        blob.markStale();

        return true;
      } catch (Exception e) {
        throw new BlobStoreException(e, blobId);
      } finally {
        lock.unlock();
      }
    } finally {
      timerlog.debug("delete(...) took: " + stopwatch);
    }
  }

  @Override
  @Guarded(by = STARTED)
  public boolean deleteHard(final BlobId blobId) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      checkNotNull(blobId);

      try {
        log.debug("Hard deleting blob {}", blobId);

        String attributePath = attributePath(blobId);
        SwiftBlobAttributes blobAttributes = new SwiftBlobAttributes(swift, getConfiguredContainer(), attributePath);
        Long contentSize = getContentSizeForDeletion(blobAttributes);

        String blobPath = contentPath(blobId);

        boolean blobDeleted = autoRetry(() -> delete(blobPath));
        autoRetry(() -> delete(attributePath));

        if (blobDeleted && contentSize != null) {
          storeMetrics.recordDeletion(contentSize);
        }

        return blobDeleted;
      }
      catch (IOException e) {
        throw new BlobStoreException(e, blobId);
      }
      finally {
        liveBlobs.invalidate(blobId);
      }
    } finally {
      timerlog.debug("deleteHard(...) took: " + stopwatch);
    }
  }

  @Nullable
  private Long getContentSizeForDeletion(final SwiftBlobAttributes blobAttributes) {
    try {
      blobAttributes.load();
      return blobAttributes.getMetrics() != null ? blobAttributes.getMetrics().getContentSize() : null;
    }
    catch (Exception e) {
      log.warn("Unable to load attributes {}, delete will not be added to metrics.", blobAttributes, e);
      return null;
    }
  }

  @Override
  @Guarded(by = STARTED)
  public BlobStoreMetrics getMetrics() {
    return autoRetry(() -> storeMetrics.getMetrics());
  }

  @Override
  @Guarded(by = STARTED)
  public synchronized void compact() {
    compact(null);
  }

  @Override
  @Guarded(by = STARTED)
  public synchronized void compact(@Nullable final BlobStoreUsageChecker inUseChecker) {
      // no-op
  }

  @Override
  public BlobStoreConfiguration getBlobStoreConfiguration() {
    return this.blobStoreConfiguration;
  }

  @Override
  public void init(final BlobStoreConfiguration configuration) {
    this.blobStoreConfiguration = configuration;
    tries.set(Integer.valueOf(String.valueOf(blobStoreConfiguration.attributes(CONFIG_KEY).get(TRIES_KEY))));
    try {
      this.swift = swiftClientFactory.create(configuration);
      autoRetry(() -> {
        if (!swift.getContainer(getConfiguredContainer()).exists()) {
          swift.getContainer(getConfiguredContainer()).create();
        }
      });
      setConfiguredContainer(getConfiguredContainer());
    } catch (Exception e) {
      throw new BlobStoreException("Unable to initialize blob store bucket: " + getConfiguredContainer(), e, null);
    }
  }

  private boolean delete(final String path) throws IOException {
    swift.getContainer(getConfiguredContainer()).getObject(path).delete();
    // note: no info returned from swift
    return true;
  }

  private void deleteQuietly(final String path) {
    swift.getContainer(getConfiguredContainer()).getObject(path).delete();
  }

  private void setConfiguredContainer(final String container) {
    blobStoreConfiguration.attributes(CONFIG_KEY).set(CONTAINER_KEY, container);
  }

  private String getConfiguredContainer() {
    return blobStoreConfiguration.attributes(CONFIG_KEY).require(CONTAINER_KEY).toString();
  }

  /**
   * Delete files known to be part of the SwiftBlobStore implementation if the content directory is empty.
   */
  @Override
  @Guarded(by = {NEW, STOPPED, FAILED})
  public void remove() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      try {
        autoRetry(() -> {
          boolean contentEmpty = swift.getContainer(getConfiguredContainer()).listDirectory(CONTENT_DIRECTORY).isEmpty();
          if (contentEmpty) {
            SwiftPropertiesFile metadata = new SwiftPropertiesFile(swift, getConfiguredContainer(), null, METADATA_FILENAME);
            metadata.remove();
            storeMetrics.remove();
            swift.getContainer(getConfiguredContainer()).delete();
          } else {
            log.warn("Unable to delete non-empty blob store content directory in bucket {}", getConfiguredContainer());
          }
        });
      } catch (IOException e) {
          throw new BlobStoreException(e, null);
      }
    } finally {
      timerlog.debug("remove(...) took: " + stopwatch);
    }
  }

  class SwiftBlob extends BlobSupport {
    SwiftBlob(final BlobId blobId) {
      super(blobId);
    }

    @Override
    public InputStream getInputStream() {
      StoredObject object = swift.getContainer(getConfiguredContainer()).getObject(contentPath(getId()));
      return autoRetry(() -> object.downloadObjectAsInputStream());
    }
  }

  private interface BlobIngester {
    StreamMetrics ingestTo(final String destination) throws IOException;
  }

  @Override
  public Stream<BlobId> getBlobIdStream() {
    Collection<DirectoryOrObject> summaries = autoRetry(() -> swift.getContainer(getConfiguredContainer()).listDirectory(CONTENT_DIRECTORY));
    return blobIdStream(summaries);
  }

  @Override
  public Stream<BlobId> getDirectPathBlobIdStream(final String prefix) {
    String subpath = format("%s/%s/%s", CONTENT_PREFIX, DIRECT_PATH_ROOT, prefix);
    Collection<DirectoryOrObject> summaries = autoRetry(() -> swift.getContainer(getConfiguredContainer()).listDirectory(new Directory(subpath, '/')));
    return blobIdStream(summaries);
  }

  private Stream<BlobId> blobIdStream(Collection<DirectoryOrObject> summaries) {
    return summaries.stream()
            .filter(element -> element.isObject())
            .map(DirectoryOrObject::getAsObject)
            .map(StoredObject::getPath)
            .map(path -> path.substring(path.lastIndexOf('/') + 1, path.length()))
            .filter(filename -> filename.endsWith(BLOB_ATTRIBUTE_SUFFIX) && !filename.startsWith(TEMPORARY_BLOB_ID_PREFIX))
            .map(filename -> filename.substring(0, filename.length() - BLOB_ATTRIBUTE_SUFFIX.length()))
            .map(BlobId::new);
  }

  @Override
  public BlobAttributes getBlobAttributes(final BlobId blobId) {
    try {
      SwiftBlobAttributes blobAttributes = new SwiftBlobAttributes(swift, getConfiguredContainer(), attributePath(blobId));
      return autoRetry(() -> blobAttributes.load()) ? blobAttributes : null;
    } catch (IOException e) {
      log.error("Unable to load S3BlobAttributes for blob id: {}", blobId, e);
      return null;
    }
  }

  @Override
  public void setBlobAttributes(BlobId blobId, BlobAttributes blobAttributes) {
    try {
      SwiftBlobAttributes swiftBlobAttributes = (SwiftBlobAttributes) getBlobAttributes(blobId);
      swiftBlobAttributes.updateFrom(blobAttributes);
      autoRetry(swiftBlobAttributes::store);
    }
    catch (Exception e) {
      log.error("Unable to set BlobAttributes for blob id: {}, exception: {}",
          blobId, e.getMessage(), log.isDebugEnabled() ? e : null);
    }
  }

  @FunctionalInterface
  protected interface Runnable<T extends Exception> {
    void run() throws T;
  }

  @FunctionalInterface
  protected interface Callable<Type, T extends Exception> {
    Type call() throws T;
  }

  protected <Thrown extends Exception> void autoRetry(Runnable<Thrown> action) throws Thrown {
    int tries = this.tries.get();
    for(int i = 0; i < tries-1; i++) {
      try {
        action.run();
        return;
      } catch (Throwable e) {
        log.debug("Operation failed (try: "+i+")", e);
      }
    }
    action.run();
  }

  protected <Type, Thrown extends Exception> Type autoRetry(Callable<Type, Thrown> action) throws Thrown {
    int tries = this.tries.get();
    for(int i = 0; i < tries-1; i++) {
      try {
        return action.call();
      } catch (Throwable e) {
        log.debug("Operation failed (try: "+i+")", e);
      }
    }
    return action.call();
  }
}
