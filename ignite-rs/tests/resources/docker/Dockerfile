# Custom Apache Ignite test image for ignite-rs integration tests.
# Extends the official image with pre-compiled Java classes for compute,
# service invocation, and entry processor tests.
#
# Build:
#   docker build -t ignite-rs-test:latest -f Dockerfile ..
#
# The build context should be the resources/ directory so that java/ sources are accessible.

ARG IGNITE_VERSION=2.15.0
FROM apacheignite/ignite:${IGNITE_VERSION}

# Copy Java sources and compile them against the Ignite classpath
COPY java/ /tmp/java-src/

RUN mkdir -p /tmp/java-classes && \
    javac -cp "/opt/ignite/apache-ignite/libs/*:/opt/ignite/apache-ignite/libs/ignite-indexing/*" \
        -d /tmp/java-classes \
        /tmp/java-src/*.java && \
    jar cf /opt/ignite/apache-ignite/libs/ignite-rs-test-classes.jar \
        -C /tmp/java-classes . && \
    rm -rf /tmp/java-src /tmp/java-classes

# The JAR is now on the default Ignite classpath and classes are available
# for compute tasks, services, and entry processors.
