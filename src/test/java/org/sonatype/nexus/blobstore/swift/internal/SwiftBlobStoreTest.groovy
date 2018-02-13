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
package org.sonatype.nexus.blobstore.swift.internal

import org.javaswift.joss.model.Account
import org.javaswift.joss.model.Container
import org.javaswift.joss.model.StoredObject
import org.sonatype.nexus.blobstore.BlobIdLocationResolver
import org.sonatype.nexus.blobstore.api.BlobId
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration
import spock.lang.Specification

import static java.util.UUID.randomUUID

/**
 * {@link SwiftBlobStore} tests.
 */
class SwiftBlobStoreTest
    extends Specification
{

  SwiftClientFactory swiftClientFactory = Mock()
  BlobIdLocationResolver locationResolver = Mock()
  SwiftBlobStoreMetricsStore storeMetrics = Mock()
  Account swift = Mock()
  SwiftBlobStore blobStore = new SwiftBlobStore(swiftClientFactory, locationResolver, storeMetrics)

  def config = new BlobStoreConfiguration()

  def attributesContents = """\
        |#Thu Jun 01 23:10:55 UTC 2017
        |@BlobStore.created-by=admin
        |size=11
        |@Container.repo-name=test
        |creationTime=1496358655289
        |@BlobStore.content-type=text/plain
        |@BlobStore.blob-name=test
        |sha1=eb4c2a5a1c04ca2d504c5e57e1f88cef08c75707
      """.stripMargin()

  def blobContent = """\
        |This is a very special blob
      """.stripMargin()

  def setup() {
    locationResolver.getLocation(_) >> { args -> args[0].toString() }
    swiftClientFactory.create(_) >> swift
    config.attributes = [swift: [container: 'myContainer']]
  }

  def "Get blob"() {
    given: 'A mocked SWIFT setup'
      def attributesSwiftObject = mockSwiftObject(attributesContents)
      def contentSwiftObject = mockSwiftObject('hello world')
      def container = mockContainer('myContainer')
      def metadataSwiftObject = mockSwiftObject();
      swift.getContainer('myContainer') >> container
      container.getObject('metadata.properties') >> metadataSwiftObject
      container.getObject('content/test.properties') >> attributesSwiftObject
      container.getObject('content/test.bytes') >> contentSwiftObject

    when: 'An existing blob is read'
      blobStore.init(config)
      blobStore.doStart()
      def blob = blobStore.get(new BlobId('test'))

    then: 'The contents are read from swift'
      blob.inputStream.text == 'hello world'
  }

  def 'Put blob'() {
    given: 'A mocked SWIFT setup'
      def container = mockContainer('myContainer')
      def metadataSwiftObject = mockSwiftObject();
      swift.getContainer('myContainer') >> container
      container.getObject('metadata.properties') >> metadataSwiftObject
      blobStore.init(config)
      blobStore.doStart()
      def headers = new HashMap()
      headers.put("BlobStore.blob-name", "testBlob")
      headers.put("BlobStore.content-type", "text/plain")
      headers.put("BlobStore.created-by","test-admin")
      locationResolver.fromHeaders(_) >> new BlobId("blob-id")
      def targetObject = mockSwiftObject()
      1 * container.getObject("content/blob-id.bytes") >> targetObject
      1 * targetObject.uploadObject(_)
      def targetPropertiesObject = mockSwiftObject()
      1 * container.getObject("content/blob-id.properties") >> targetPropertiesObject
      1 * targetPropertiesObject.uploadObject(_)

    when: 'blob is added'
      def blob = blobStore.create(new ByteArrayInputStream(blobContent.bytes), headers)

    then: 'blob is created'
      blob != null
  }

  def 'delete successful'() {
    given: 'blob exists'
      def container = mockContainer('myContainer')
      def metadataSwiftObject = mockSwiftObject();
      swift.getContainer('myContainer') >> container
      container.getObject('metadata.properties') >> metadataSwiftObject
      blobStore.init(config)
      blobStore.doStart()
      def attributesS3Object = mockSwiftObject(attributesContents)
      container.getObject('content/soft-delete-success.properties') >> attributesS3Object
      2 * container.getObject('content/soft-delete-success.bytes') >> mockSwiftObject("random")
      1 * container.getObject('content/soft-delete-success.bytes').delete()

    when: 'blob is deleted'
      def deleted = blobStore.delete(new BlobId('soft-delete-success'), 'successful test')

    then: 'deleted is true'
      deleted == true
  }

  def 'delete returns false when blob does not exist'() {
    given: 'blob store setup'
      def container = mockContainer('myContainer')
      def metadataSwiftObject = mockSwiftObject();
      swift.getContainer('myContainer') >> container
      container.getObject('metadata.properties') >> metadataSwiftObject
      blobStore.init(config)
      blobStore.doStart()
      container.getObject('content/soft-delete-fail.properties') >> mockSwiftObject()

    when: 'nonexistent blob is deleted'
      def deleted = blobStore.delete(new BlobId('soft-delete-fail'), 'test')

    then: 'deleted tag is added'
      deleted == false
  }

  private Container mockContainer(String s) {
    Container container = Mock()
    container.exists() >> true
    container
  }

  private mockSwiftObject() {
    mockSwiftObject(null)
  }

  private mockSwiftObject(String contents) {
    StoredObject swiftObject = Mock()
    swiftObject.downloadObjectAsInputStream() >> ((contents == null) ? null : new ByteArrayInputStream(contents.bytes))
    swiftObject.exists() >> (contents != null)
    swiftObject
  }
}
