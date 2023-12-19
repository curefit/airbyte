# Currently Self-Maintained

## 1. Mongo Source Connector:

- Remove initial snapshot to avoid syncing data from inception.
- Remove keys with null values for schema validation.

- ```markdown
  ./gradlew :airbyte-integrations:connectors:source-mongodb-v2:buildConnectorImage

## 2. MySQL Source Connector:

- Remove initial snapshot to avoid syncing data from inception.

- ```markdown
  ./gradlew :airbyte-integrations:connectors:source-mysql:buildConnectorImage

## 3. Normalization Code:

- Maintain base normalization code to control the transformation layer.
- Change the code flow from Github based to AWS S3 based

- ```markdown
  ./gradlew :airbyte-integrations:bases:base-normalization:airbyteDocker
