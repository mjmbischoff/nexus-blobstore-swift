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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Persistent properties file stored in Openstack swift.
 */
public class SwiftPropertiesFile extends Properties {
  private static final Logger log = LoggerFactory.getLogger(SwiftPropertiesFile.class);
  private final Account swift;
  private final String container;
  private final String key;

  public SwiftPropertiesFile(final Account swift, final String container, final String name) {
    this.swift = checkNotNull(swift);
    this.container = checkNotNull(container);
    this.key = checkNotNull(name);
  }

  public void load() throws IOException {
    log.debug("Loading: {}/{}", container, key);

    StoredObject object = swift.getContainer(container).getObject(key);
    try (InputStream inputStream = object.downloadObjectAsInputStream()) {
      load(inputStream);
    }
  }

  public void store() throws IOException {
    log.debug("Storing: {}/{}", container, key);

    ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
    store(bufferStream, null);
    byte[] buffer = bufferStream.toByteArray();

    StoredObject object = swift.getContainer(container).getObject(key);
    object.setContentLength(buffer.length);
    object.uploadObject(new ByteArrayInputStream(buffer));
  }

  public boolean exists() throws IOException {
    return swift.getContainer(container).getObject(key).exists();
  }

  public void remove() throws IOException {
    swift.getContainer(container).getObject(key).delete();
  }

  public String toString() {
    return getClass().getSimpleName() + "{" +
        "container=" + container +
        ", key=" + key +
        '}';
  }
}
