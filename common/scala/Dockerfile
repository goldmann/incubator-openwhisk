# Latest JDK 8 image from Mar 22nd
FROM jboss/base-jdk@sha256:46732f50e620e720806488d63ea69aa3499f27d7082e69c1a041326bfcf0ec0e

USER root

RUN mkdir /logs && chmod 777 /logs

COPY transformEnvironment.sh /
RUN chmod +x /transformEnvironment.sh

COPY copyJMXFiles.sh /
RUN chmod +x /copyJMXFiles.sh

WORKDIR /

USER 1000

