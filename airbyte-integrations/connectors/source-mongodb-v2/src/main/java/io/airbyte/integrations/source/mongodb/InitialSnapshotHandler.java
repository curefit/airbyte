/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import io.airbyte.commons.exceptions.ConfigErrorException;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.mongodb.cdc.MongoDbCdcConnectorMetadataInjector;
import io.airbyte.integrations.source.mongodb.state.IdType;
import io.airbyte.integrations.source.mongodb.state.MongoDbStateManager;
import io.airbyte.integrations.source.mongodb.state.MongoDbStreamState;
import io.airbyte.integrations.source.mongodb.MongoDbSourceConfig;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.CatalogHelpers;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.SyncMode;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Date;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves iterators used for the initial snapshot
 */
public class InitialSnapshotHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(InitialSnapshotHandler.class);

  /**
   * For each given stream configured as incremental sync it will output an iterator that will
   * retrieve documents from the given database. Each iterator will start after the last checkpointed
   * document, if any, or from the beginning of the stream otherwise.
   */

  public static S3Object getS3Object(String S3_FILE_PATH) {
      int bucketEndIndex = S3_FILE_PATH.indexOf("/", 5);

      String bucketName = "";
      String objectKey = "";

      if (bucketEndIndex != -1) {

          bucketName = S3_FILE_PATH.substring(5, bucketEndIndex);
          objectKey = S3_FILE_PATH.substring(bucketEndIndex + 1);

          System.out.println("Bucket Name: " + bucketName);
          System.out.println("Object Key: " + objectKey);
      } else {
          System.out.println("Not a valid S3 URI");
      }

      AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion("ap-south-1").build();

      // Specify the bucket name and object key
      GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, objectKey);

      // Get the S3 object
      S3Object s3Object = s3.getObject(getObjectRequest);

      return s3Object;

  }

    private String extractSnapshotCollectionName(String inputString) {
        String patternString = "snapshot\\.select\\.statement\\.overrides\\.(.*?)=";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(inputString);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    private long extractSnapshotOverrideFilter(String sqlQuery) {
        String patternString = "=(.*?)$";
        Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sqlQuery);

        if (matcher.find()) {
            return Long.parseLong(matcher.group(1).trim());
        }
        return 15L;
    }

    private boolean isMatchingTableName(String snapshotTableName, String tableName) {
        return snapshotTableName != null && snapshotTableName.equals(tableName);
    }

    private List<Object> getSnapshotOverrideQueryIfExists(String collectionName, String S3_FILE_PATH) {

        String snapShotOverrideCollection = "";
        long snapshotOverrideDays = 15L;

        try {

            S3Object s3Object = getS3Object(S3_FILE_PATH);

            // Read the content of the object
            S3ObjectInputStream objectInputStream = s3Object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectInputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                String snapshotCollectionName = extractSnapshotCollectionName(line);
                if (isMatchingTableName(snapshotCollectionName, collectionName)) {
                    snapShotOverrideCollection = collectionName;
                    snapshotOverrideDays = extractSnapshotOverrideFilter(line);
                    break;
                }
            }
            reader.close();
            objectInputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Object> result = new ArrayList<>();
        result.add(snapShotOverrideCollection);
        result.add(snapshotOverrideDays);
        return result;

    }

  public List<AutoCloseableIterator<AirbyteMessage>> getIterators(
                                                                  final List<ConfiguredAirbyteStream> streams,
                                                                  final MongoDbStateManager stateManager,
                                                                  final MongoDatabase database,
                                                                  final MongoDbCdcConnectorMetadataInjector cdcConnectorMetadataInjector,
                                                                  final Instant emittedAt,
                                                                  final int checkpointInterval,
                                                                  String S3_FILE_PATH,
                                                                  final MongoClient mongoClient) {
    return streams
        .stream()
        .peek(airbyteStream -> {
          if (!airbyteStream.getSyncMode().equals(SyncMode.INCREMENTAL))
            LOGGER.warn("Stream {} configured with unsupported sync mode: {}", airbyteStream.getStream().getName(), airbyteStream.getSyncMode());
        })
        .filter(airbyteStream -> airbyteStream.getSyncMode().equals(SyncMode.INCREMENTAL))
        .map(airbyteStream -> {
          final var collectionName = airbyteStream.getStream().getName();
          final var collection = database.getCollection(collectionName);

          final var fields = Projections.fields(Projections.include(CatalogHelpers.getTopLevelFieldNames(airbyteStream).stream().toList()));



            /**
             * Skip checking _id types, as the query takes lot of time for big collections
             *
             */
//          final var idTypes = aggregateIdField(collection);
//          if (idTypes.size() > 1) {
//            throw new ConfigErrorException("The _id fields in a collection must be consistently typed (collection = " + collectionName + ").");
//          }
//
//
//          idTypes.stream().findFirst().ifPresent(idType -> {
//            if (IdType.findByBsonType(idType).isEmpty()) {
//              throw new ConfigErrorException("Only _id fields with the following types are currently supported: " + IdType.SUPPORTED
//                  + " (collection = " + collectionName + ").");
//            }
//          });

          // find the existing state, if there is one, for this stream
          final Optional<MongoDbStreamState> existingState =
              stateManager.getStreamState(airbyteStream.getStream().getName(), airbyteStream.getStream().getNamespace());


          // Get _id from last 15 days incase if the collection is present in the override file
          final Bson filter ;

          List<Object> result = getSnapshotOverrideQueryIfExists(collectionName, S3_FILE_PATH) ;

          if((String) result.get(0) != "") {
              filter = Filters.gte("updatedDate", new Date(System.currentTimeMillis() - ((long) result.get(1) * 24L * 60L * 60L * 1000L)));
          } else {
              filter = existingState
                      .map(state -> Filters.gt(MongoConstants.ID_FIELD, new ObjectId(state.id())))
                      .orElseGet(BsonDocument::new);
          }


          // The filter determines the starting point of this iterator based on the state of this collection.
          // If a state exists, it will use that state to create a query akin to
          // "where _id > [last saved state] order by _id ASC".
          // If no state exists, it will create a query akin to "where 1=1 order by _id ASC"
//          final Bson filter = existingState
//              // TODO add type support here when we add support for _id fields that are not ObjectId types
//              .map(state -> Filters.gt(MongoConstants.ID_FIELD, new ObjectId(state.id())))
//              // if nothing was found, return a new BsonDocument
//              .orElseGet(BsonDocument::new);

          LOGGER.info("filter {} override for snapshot in collection {}", filter, collectionName);

          final var cursor = collection.find()
              .filter(filter)
              .projection(fields)
              .sort(Sorts.ascending(MongoConstants.ID_FIELD))
              .allowDiskUse(true)
              .cursor();

          final var stateIterator =
              new MongoDbStateIterator(cursor, stateManager, Optional.ofNullable(cdcConnectorMetadataInjector),
                  airbyteStream, emittedAt, checkpointInterval, MongoConstants.CHECKPOINT_DURATION);
          return AutoCloseableIterators.fromIterator(stateIterator, cursor::close, null);
        })
        .toList();
  }

  /**
   * Returns a list of types (as strings) that the _id field has for the provided collection.
   *
   * @param collection Collection to aggregate the _id types of.
   * @return List of bson types (as strings) that the _id field contains.
   */
  private List<String> aggregateIdField(final MongoCollection<Document> collection) {
    final List<String> idTypes = new ArrayList<>();
    /*
     * Sanity check that all ID_FIELD values are of the same type for this collection.
     * db.collection.aggregate([{ $group : { _id : { $type : "$_id" }, count : { $sum : 1 } } }])
     */
    collection.aggregate(List.of(
        Aggregates.group(
            new Document(MongoConstants.ID_FIELD, new Document("$type", "$_id")),
            Accumulators.sum("count", 1))))
        .forEach(document -> {
          // the document will be in the structure of
          // {"_id": {"_id": "[TYPE]"}, "count": [COUNT]}
          // where [TYPE] is the bson type (objectId, string, etc.) and [COUNT] is the number of documents of
          // that type
          final Document innerDocument = document.get(MongoConstants.ID_FIELD, Document.class);
          idTypes.add(innerDocument.get(MongoConstants.ID_FIELD).toString());
        });

    return idTypes;
  }

}
