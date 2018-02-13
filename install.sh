#!/bin/bash -e

export SWIFT_BLOBSTORE_VERSION=1.0.0-SNAPSHOT
export NEXUS_HOME=../nexus-professional-3.8.0-02
# export NEXUS_HOME=../nexus-internal/target/nexus-professional-3.7.0-SNAPSHOT

mkdir -p ${NEXUS_HOME}/system/org/sonatype/nexus/nexus-blobstore-swift/${SWIFT_BLOBSTORE_VERSION}/
cp target/nexus-blobstore-swift-*.jar ${NEXUS_HOME}/system/org/sonatype/nexus/nexus-blobstore-swift/${SWIFT_BLOBSTORE_VERSION}/

sed -i.bak -e "/nexus-blobstore-file/a\\"$'\n'"<bundle>mvn:org.sonatype.nexus/nexus-blobstore-swift/${SWIFT_BLOBSTORE_VERSION}</bundle>" ${NEXUS_HOME}/system/org/sonatype/nexus/assemblies/nexus-base-feature/*/nexus-base-feature-*-features.xml
sed -i.bak -e "/nexus-blobstore-file/a\\"$'\n'"<bundle>mvn:org.sonatype.nexus/nexus-blobstore-swift/${SWIFT_BLOBSTORE_VERSION}</bundle>" ${NEXUS_HOME}/system/org/sonatype/nexus/assemblies/nexus-core-feature/*/nexus-core-feature-*-features.xml
