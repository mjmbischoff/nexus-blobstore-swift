FROM sonatype/nexus3:3.13.0

ENV SWIFT_BLOBSTORE_VERSION 1.0.0-SNAPSHOT
ENV NEXUS_HOME /opt/sonatype/nexus

COPY target/nexus-blobstore-swift-${SWIFT_BLOBSTORE_VERSION}.jar ${NEXUS_HOME}/system/org/sonatype/nexus/nexus-blobstore-swift/${SWIFT_BLOBSTORE_VERSION}/

USER root

RUN sed -i.bak \
  -e "/nexus-blobstore-file/a\\"$'\n'"<bundle>mvn:org.sonatype.nexus/nexus-blobstore-swift/${SWIFT_BLOBSTORE_VERSION}</bundle>" \
  ${NEXUS_HOME}/system/org/sonatype/nexus/assemblies/nexus-base-feature/*/nexus-base-feature-*-features.xml \
  ${NEXUS_HOME}/system/org/sonatype/nexus/assemblies/nexus-core-feature/*/nexus-core-feature-*-features.xml

USER nexus

EXPOSE 8081