#!/bin/bash

# NOTE - this is currently a manual only integration test. e.g. you must manually setup the conditions and manually assert correctness. See README for some hints on how to do this.

# This is intended to be run the plugin's root directory.
# Ensure you have Docker installed locally and set the ELASTIC_STACK_VERSION environment variable.
# Usage: `ci/integration/docker-test.sh no_storage.config`
set -e

if [ "$ELASTIC_STACK_VERSION" ]; then
    echo "Testing against version: $ELASTIC_STACK_VERSION"

    if [[ "$ELASTIC_STACK_VERSION" = *"-SNAPSHOT" ]]; then
        cd /tmp
        wget https://snapshots.elastic.co/docker/logstash-"$ELASTIC_STACK_VERSION".tar.gz
        tar xfvz logstash-"$ELASTIC_STACK_VERSION".tar.gz  repositories
        echo "Loading docker image: "
        cat repositories
        docker load < logstash-"$ELASTIC_STACK_VERSION".tar.gz
        rm logstash-"$ELASTIC_STACK_VERSION".tar.gz
        cd -
    fi

    if [ -f Gemfile.lock ]; then
        rm Gemfile.lock
    fi

    # Build the gem
    find . -name *.gemspec | xargs gem build
    cp *.gem ci/integration/this.gem
    source ci/integration/env.sh

    export LS_ARGS="-f $@"
    docker-compose -f ci/integration/docker-compose.yml down
    docker-compose -f ci/integration/docker-compose.yml up --build --exit-code-from logstash1 --force-recreate
    # Need to manually stop
else
    echo "Please set the ELASTIC_STACK_VERSION environment variable"
    echo "For example: export ELASTIC_STACK_VERSION=6.2.4"
    exit 1
fi

