FROM bitnami/spark:3.2.1
LABEL maintainer="Dennis Pfisterer, http://dennis-pfisterer.de"
WORKDIR /app

#------------------------------------
# Set execution environment

# Java Dependencies for Spark Kafka
ENV SPARK_VERSION "3.2.1"
ENV SPARK_KAFKA_DEPENDENCY "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}"

# Java Dependencies for MariaDB
ENV MARIADB_CLIENT_VERSION "3.0.3"
ENV MARIADB_DEPENDENCY "org.mariadb.jdbc:mariadb-java-client:${MARIADB_CLIENT_VERSION}"

# Environment variables used later to configure spark-submit
ENV EXTRA_PACKAGES "${SPARK_KAFKA_DEPENDENCY},${MARIADB_DEPENDENCY}"
ENV IVY_PACKAGE_DIR "/tmp/.ivy"
ENV IVY_SETTINGS_OFFLINE "ivy-settings-offline.xml"

#------------------------------------
# Prepare the system

USER root

# Workaround for "failure to login" error message: 
# cf. https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login/56083736
RUN groupadd --gid 1001 spark
RUN useradd --uid 1001 --gid spark --shell /bin/bash spark
# End: Workaround

RUN apt-get update && apt-get install -y zip
RUN chown spark:spark /app/

USER spark

WORKDIR /app

#------------------------------------
# Prepare dependencies in ZIP file

# Prefetch Maven dependencies by running an empty python file using spark-submit
# to benefit from Docker's build cache
RUN echo -e 'IGNORE_THIS_ERROR' > /tmp/empty.py ; spark-submit --verbose \
	--conf "spark.jars.ivy=${IVY_PACKAGE_DIR}" \
	--packages "${EXTRA_PACKAGES}" \
	/tmp/empty.py ; rm -f /tmp/empty.py

# Prepare the app's python dependencies
ADD --chown=spark:spark requirements.txt /app/
RUN pip install --no-cache-dir -t /app/dependencies -r requirements.txt
RUN echo -e "Contents of /app/dependencies:\n" ; find /app/dependencies

# Zip all dependencies
WORKDIR /app/dependencies
RUN zip -r /app/dependencies.zip .

WORKDIR /app
RUN rm -rf /app/dependencies

#------------------------------------
# Copy the application code into the container

# Copy application code
COPY --chown=spark:spark *.py /app/
COPY --chown=spark:spark ${IVY_SETTINGS_OFFLINE} /app/${IVY_SETTINGS_OFFLINE}

#------------------------------------
# Set entrypoint and use dependencies zip file

# Use YARN deployment in client mode (requires Hadoop config)
# --master yarn --deploy-mode client"

# Use YARN deployment in cluster mode (requires Hadoop config)
# --master yarn --deploy-mode cluster

# Use local deployment
# --master local

ENTRYPOINT spark-submit \
	--verbose \
	--conf "spark.jars.ivy=${IVY_PACKAGE_DIR}" \
	--conf "spark.jars.ivySettings=/app/${IVY_SETTINGS_OFFLINE}" \
	--master local \
	--packages "${EXTRA_PACKAGES}" \
	--py-files /app/dependencies.zip \
	/app/spark-app.py

CMD [""]
