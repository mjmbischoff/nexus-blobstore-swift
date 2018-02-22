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
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.common.collect.NestedAttributesMap;

import javax.inject.Named;

import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.AUTHURL_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.AUTH_METHOD;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.CONFIG_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.PASSWORD_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.SOCKET_TIMEOUT_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.TENANT_ID_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.TENANT_NAME_KEY;
import static org.sonatype.nexus.blobstore.swift.internal.SwiftBlobStore.USERNAME_KEY;

/**
 * Creates configured Swift {@link Account} respresenting a openstack swift client.
 */
@Named
public class SwiftClientFactory {

  private static final int DEFAULT_SOCKET_TIMEOUT = 5000;

  public Account create(final BlobStoreConfiguration blobStoreConfiguration) {
    NestedAttributesMap config = blobStoreConfiguration.attributes(CONFIG_KEY);
    String socketTimeout = config.get(SOCKET_TIMEOUT_KEY, String.class);
    String username = config.get(USERNAME_KEY, String.class);
    String password = config.get(PASSWORD_KEY, String.class);
    String authUrl = config.get(AUTHURL_KEY, String.class);
    AccountFactory factory = new AccountFactory()
            .setSocketTimeout(parseSocketTimeout(socketTimeout))
            .setUsername(username)
            .setPassword(password)
            .setAuthUrl(authUrl);

    AuthenticationMethod authenticationMethod = toAuthMethod(config.get(AUTH_METHOD, String.class));
    factory.setAuthenticationMethod(authenticationMethod);

    String tenantId = config.get(TENANT_ID_KEY, String.class);
    if(!Strings.isNullOrEmpty(tenantId)) {
      factory.setTenantId(tenantId);
    } else {
      String tenantName = config.get(TENANT_NAME_KEY, String.class);
      if (!Strings.isNullOrEmpty(tenantName)){
        factory.setTenantName(tenantName);
      }
    }

    return factory.createAccount();
  }

  private int parseSocketTimeout(String socketTimeout) {
    try {
      return Integer.parseInt(socketTimeout);
    } catch (NumberFormatException e) {
      return DEFAULT_SOCKET_TIMEOUT;
    }
  }

  private AuthenticationMethod toAuthMethod(String value) {
    if(value==null) {
      return AuthenticationMethod.BASIC;
    }
    switch (value) {
      default: return AuthenticationMethod.BASIC;
      case "BASIC": return AuthenticationMethod.BASIC;
      case "KEYSTONE": return AuthenticationMethod.KEYSTONE;
      case "KEYSTONE_V3": return AuthenticationMethod.KEYSTONE_V3;
      case "TEMPAUTH": return AuthenticationMethod.TEMPAUTH;
    }
  }
}
