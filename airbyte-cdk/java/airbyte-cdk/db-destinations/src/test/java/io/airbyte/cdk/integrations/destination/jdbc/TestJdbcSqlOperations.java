/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.destination.jdbc;

import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.integrations.destination_async.partial_messages.PartialAirbyteMessage;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestJdbcSqlOperations extends JdbcSqlOperations {

  @Override
  public void insertRecordsInternal(final JdbcDatabase database,
                                    final List<PartialAirbyteMessage> records,
                                    final String schemaName,
                                    final String tableName)
      throws Exception {
    // Not required for the testing
  }

  @Test
  public void testCreateSchemaIfNotExists() {
    final JdbcDatabase db = Mockito.mock(JdbcDatabase.class);
    final var schemaName = "foo";
    try {
      Mockito.doThrow(new SQLException("TEST")).when(db).execute(Mockito.anyString());
    } catch (final Exception e) {
      // This would not be expected, but the `execute` method above will flag as an unhandled exception
      assert false;
    }
    final SQLException exception = Assertions.assertThrows(SQLException.class, () -> createSchemaIfNotExists(db, schemaName));
    Assertions.assertEquals(exception.getMessage(), "TEST");
  }

}
