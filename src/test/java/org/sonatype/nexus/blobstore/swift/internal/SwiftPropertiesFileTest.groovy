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

import org.apache.commons.io.IOUtils
import org.javaswift.joss.model.Account
import org.javaswift.joss.model.Container
import org.javaswift.joss.model.StoredObject
import spock.lang.Specification


/**
 * {@link SwiftPropertiesFile} tests.
 */
public class SwiftPropertiesFileTest
    extends Specification
{

  Account swift = Mock()

  String testProperties = 'propertyName = value\n'

  def "Load ingests properties from swift object"() {
    given:
      def container = mockContainer('myContainer')
      swift.getContainer('myContainer') >> container
      SwiftPropertiesFile propertiesFile = new SwiftPropertiesFile(swift, 'myContainer', 'mykey')
      StoredObject swiftObject = Mock()

    when:
      propertiesFile.load()

    then:
      propertiesFile.getProperty('propertyName') == 'value'
      1 * swift.getContainer('myContainer').getObject('mykey') >> swiftObject
      1 * swiftObject.downloadObjectAsInputStream() >> new ByteArrayInputStream(testProperties.bytes)
  }

  def "Store writes properties to swift object"() {
    given:
      def container = mockContainer('myContainer')
      swift.getContainer('myContainer') >> container
      SwiftPropertiesFile propertiesFile = new SwiftPropertiesFile(swift, 'myContainer', 'mykey')
      StoredObject swiftObject = Mock()

    when:
      propertiesFile.setProperty('testProperty', 'newValue')
      propertiesFile.store()

    then:
      1 * swift.getContainer('myContainer').getObject('mykey') >> swiftObject
      1 * swiftObject.uploadObject(_) >> { InputStream inputstream ->
        def text = inputstream.text
        assert text.contains('testProperty=newValue' + System.lineSeparator())
      }
  }

  private Container mockContainer(String s) {
    Container container = Mock()
    container.exists() >> true
    container
  }
}

