# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM openjdk:8-jre-alpine
LABEL maintainer="Dan Chaffelson <chaffelson@gmail.com>"
LABEL site="https://github.com/Chaffelson/nipyapi"

ARG SCHEMA_VERSION=0.5.2
ARG REG_USER=registry
ARG TARGET_DIR=/opt/hortonworks-registry

ENV SCHEMA_DIR_NAME=hortonworks-registry-${SCHEMA_VERSION}
ENV SCHEMA_BINARY_FILENAME=${SCHEMA_DIR_NAME}.tar.gz
ENV SCHEMA_DOWNLOAD_BASE_URL=https://github.com/hortonworks/registry/releases/download
ENV SCHEMA_BINARY_URL=${SCHEMA_DOWNLOAD_BASE_URL}/v${SCHEMA_VERSION}/${SCHEMA_BINARY_FILENAME}
ENV REGISTRY_HOME=${TARGET_DIR}/${SCHEMA_DIR_NAME}

RUN mkdir -p ${TARGET_DIR} \
    && apk --update add wget bash \
    && wget ${SCHEMA_BINARY_URL} -O ${TARGET_DIR}/${SCHEMA_BINARY_FILENAME} \
    && tar -xzf ${TARGET_DIR}/${SCHEMA_BINARY_FILENAME} -C ${TARGET_DIR} \
    && cp ${REGISTRY_HOME}/conf/registry-inmemory-example.yaml ${REGISTRY_HOME}/conf/registry.yaml \
    && rm ${TARGET_DIR}/${SCHEMA_BINARY_FILENAME}

WORKDIR ${REGISTRY_HOME}

EXPOSE 9090 9091

CMD ["./bin/registry-server-start.sh","./conf/registry.yaml"]
