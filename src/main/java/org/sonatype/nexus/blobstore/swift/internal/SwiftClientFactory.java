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

import com.google.common.base.Strings;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;

import javax.inject.Named;

import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.AUTHURL_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.CONFIG_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.PASSWORD_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.TENANT_ID_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.TENANT_NAME_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.USERNAME_KEY;

/**
 * Creates configured Swift {@link Account} respresenting a openstack swift client.
 */
@Named
public class SwiftClientFactory {

  public Account create(final BlobStoreConfiguration blobStoreConfiguration) {
    String username = blobStoreConfiguration.attributes(CONFIG_KEY).get(USERNAME_KEY, String.class);
    String password = blobStoreConfiguration.attributes(CONFIG_KEY).get(PASSWORD_KEY, String.class);
    String authUrl = blobStoreConfiguration.attributes(CONFIG_KEY).get(AUTHURL_KEY, String.class);
    AccountFactory factory = new AccountFactory()
            .setUsername(username)
            .setPassword(password)
            .setAuthUrl(authUrl);

    String tenantId = blobStoreConfiguration.attributes(CONFIG_KEY).get(TENANT_ID_KEY, String.class);
    if(!Strings.isNullOrEmpty(tenantId)) {
      factory.setTenantId(tenantId);
    } else {
      String tenantName = blobStoreConfiguration.attributes(CONFIG_KEY).get(TENANT_NAME_KEY, String.class);
      factory.setTenantName(tenantName);
    }

    return factory.createAccount();
  }
}
