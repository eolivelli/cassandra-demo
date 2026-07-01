package com.example.store;

import com.example.store.schema.SchemaManager;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fast, Docker-free coverage of the index template rendering. */
class IndexTemplateTest {

    @Test
    void rendersOneStatementPerTableWithComputedNames() {
        // A null session is fine: buildIndexStatements does not touch Cassandra.
        SchemaManager schema = new SchemaManager(null, "shop");
        String template = "CREATE CUSTOM INDEX %s ON %s.%s (email) USING 'StorageAttachedIndex'";

        List<String> statements = schema.buildIndexStatements(template);

        assertEquals(SchemaManager.TABLE_NAMES.size(), statements.size());
        assertEquals(
                "CREATE CUSTOM INDEX index_customers ON shop.customers (email) USING 'StorageAttachedIndex'",
                statements.get(0));
        // Placeholder order is index name, keyspace, table.
        for (int i = 0; i < statements.size(); i++) {
            String table = SchemaManager.TABLE_NAMES.get(i);
            assertTrue(statements.get(i).contains("index_" + table),
                    "index name should be index_" + table);
            assertTrue(statements.get(i).contains("shop." + table),
                    "should target shop." + table);
        }
    }

    @Test
    void supportsEscapedPercentInOptions() {
        SchemaManager schema = new SchemaManager(null, "shop");
        // %% renders to a literal % (e.g. inside WITH OPTIONS map values).
        String template = "CREATE CUSTOM INDEX %s ON %s.%s (name) USING 'x' WITH OPTIONS = {'ratio':'50%%'}";

        String first = schema.buildIndexStatements(template).get(0);

        assertTrue(first.endsWith("{'ratio':'50%'}"), first);
    }

    @Test
    void rejectsTemplateWithBadPlaceholder() {
        SchemaManager schema = new SchemaManager(null, "shop");
        // %d expects a number but gets a String -> IllegalArgumentException, wrapped.
        assertThrows(IllegalArgumentException.class,
                () -> schema.buildIndexStatements("CREATE INDEX %d ON %s.%s (email)"));
    }
}
