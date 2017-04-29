package liquibase.ext.postgresql.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.ext.postgresql.database.PostgresqlDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddUniqueConstraintGenerator;
import liquibase.statement.core.AddUniqueConstraintStatement;
import liquibase.structure.core.Index;
import liquibase.util.StringUtils;

/**
 * Created by dmd on 4/30/2017.
 */
public class AddUniqueConstraintGeneratorPostgresql extends AddUniqueConstraintGenerator {

    @Override
    public int getPriority() {
        return 5;
    }

    @Override
    public boolean supports(AddUniqueConstraintStatement statement, Database database) {
        return database instanceof PostgresqlDatabase;
    }

    @Override
    public Sql[] generateSql(AddUniqueConstraintStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        String sql = null;
        if (statement.getConstraintName() == null) {
            sql = String.format("ALTER TABLE %s ADD UNIQUE (%s)"
                    , database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                    , database.escapeColumnNameList(statement.getColumnNames())
            );
        } else {
            sql = String.format("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)"
                    , database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                    , database.escapeConstraintName(statement.getConstraintName())
                    , database.escapeColumnNameList(statement.getColumnNames())
            );
        }
        if(database instanceof OracleDatabase) {
            if (statement.isDeferrable() || statement.isInitiallyDeferred()) {
                if (statement.isDeferrable()) {
                    sql += " DEFERRABLE";
                }

                if (statement.isInitiallyDeferred()) {
                    sql +=" INITIALLY DEFERRED";
                }
            }
            if (statement.isDisabled()) {
                sql +=" DISABLE";
            }
        }

        if (StringUtils.trimToNull(statement.getTablespace()) != null && database.supportsTablespaces()) {
            if (database instanceof MSSQLDatabase) {
                sql += " ON " + statement.getTablespace();
            } else if (database instanceof DB2Database
                    || database instanceof SybaseASADatabase
                    || database instanceof InformixDatabase) {
                ; //not supported
            } else {
                sql += " USING INDEX TABLESPACE \"" + statement.getTablespace() + "\"";
            }
        }

        if (statement.getForIndexName() != null) {
            sql += " USING INDEX "+database.escapeObjectName(statement.getForIndexCatalogName(), statement.getForIndexSchemaName(), statement.getForIndexName(), Index.class);
        }

        return new Sql[] {
                new UnparsedSql(sql, getAffectedUniqueConstraint(statement))
        };

    }

}
