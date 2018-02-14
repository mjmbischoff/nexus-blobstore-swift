/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2008-present Sonatype, Inc.
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

import java.util.Arrays;
import java.util.List;

import javax.inject.Named;

import org.sonatype.goodies.i18n.I18N;
import org.sonatype.goodies.i18n.MessageBundle;
import org.sonatype.nexus.blobstore.BlobStoreDescriptor;
import org.sonatype.nexus.formfields.FormField;
import org.sonatype.nexus.formfields.NumberTextFormField;
import org.sonatype.nexus.formfields.PasswordFormField;
import org.sonatype.nexus.formfields.StringTextFormField;

/**
 * A {@link BlobStoreDescriptor} for {@link SwiftBlobStore}.
 *
 * @since 3.4
 */
@Named(SwiftBlobStoreDescriptor.TYPE)
public class SwiftBlobStoreDescriptor implements BlobStoreDescriptor {
  public static final String TYPE = "SWIFT";

  private interface Messages extends MessageBundle {
    @DefaultMessage("SWIFT")
    String name();

    @DefaultMessage("Container")
    String containerLabel();

    @DefaultMessage("Openstack swift container name")
    String containerHelp();

    @DefaultMessage("Username")
    String usernameLabel();

    @DefaultMessage("Openstack swift username")
    String usernameHelp();

    @DefaultMessage("Password")
    String passwordLabel();

    @DefaultMessage("Openstack swift password")
    String passwordHelp();

    @DefaultMessage("auth url")
    String authUrlLabel();

    @DefaultMessage("Openstack swift auth url")
    String authUrlHelp();

    @DefaultMessage("tenant Id")
    String tenantIdLabel();

    @DefaultMessage("Openstack tenant id")
    String tenantIdHelp();

    @DefaultMessage("tenant name")
    String tenantNameLabel();

    @DefaultMessage("Openstack tenant name")
    String tenantNameHelp();
  }

  private static final Messages messages = I18N.create(Messages.class);

  private final FormField container;
  private final FormField username;
  private final FormField password;
  private final FormField authUrl;
  private final FormField tenantId;
  private final FormField tenantName;

  public SwiftBlobStoreDescriptor() {
    this.container = new StringTextFormField(
        SwiftBlobStore.CONTAINER_KEY,
        messages.containerLabel(),
        messages.containerHelp(),
        FormField.MANDATORY
    );
    this.username = new StringTextFormField(
        SwiftBlobStore.USERNAME_KEY,
        messages.usernameLabel(),
        messages.usernameHelp(),
        FormField.MANDATORY
    );
    this.password = new PasswordFormField(
        SwiftBlobStore.PASSWORD_KEY,
        messages.passwordLabel(),
        messages.passwordHelp(),
        FormField.MANDATORY
    );
    this.authUrl = new StringTextFormField(
        SwiftBlobStore.AUTHURL_KEY,
        messages.authUrlLabel(),
        messages.authUrlHelp(),
        FormField.MANDATORY
    );
    this.tenantId = new StringTextFormField(
        SwiftBlobStore.TENANT_ID_KEY,
        messages.tenantIdLabel(),
        messages.tenantIdHelp(),
        FormField.OPTIONAL
    );
    this.tenantName = new StringTextFormField(
        SwiftBlobStore.TENANT_NAME_KEY,
        messages.tenantNameLabel(),
        messages.tenantNameHelp(),
        FormField.OPTIONAL
    );
  }

  @Override
  public String getName() {
    return messages.name();
  }

  @Override
  public List<FormField> getFormFields() {
      return Arrays.asList(container, username, password, authUrl, tenantId, tenantName);
  }
}