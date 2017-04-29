package liquibase.ext.postgresql.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.DB2Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.SybaseASADatabase;
import liquibase.ext.postgresql.database.PostgresqlDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddPrimaryKeyGenerator;
import liquibase.statement.core.AddPrimaryKeyStatement;
import liquibase.structure.core.Index;
import liquibase.util.StringUtils;

/**
 * Created by dmd on 4/29/2017.
 */
public class AddPrimaryKeyGeneratorPostgresql extends AddPrimaryKeyGenerator {


    @Override
    public int getPriority() {
        return 5;
    }


    @Override
    public boolean supports(AddPrimaryKeyStatement statement, Database database) {
        return database instanceof PostgresqlDatabase;
    }

    @Override
    public Sql[] generateSql(AddPrimaryKeyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql;
        if (statement.getConstraintName() == null  || database instanceof MySQLDatabase || database instanceof SybaseASADatabase) {
            sql = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " ADD PRIMARY KEY (" + database.escapeColumnNameList(statement.getColumnNames()) + ")";
        } else {
            sql = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " ADD CONSTRAINT " + database.escapeConstraintName(statement.getConstraintName())+" PRIMARY KEY";
            if (database instanceof MSSQLDatabase && !statement.isClustered()) {
                if (statement.isClustered()) {
                    sql += " CLUSTERED";
                } else {
                    sql += " NONCLUSTERED";
                }
            }
            sql += " (" + database.escapeColumnNameList(statement.getColumnNames()) + ")";
        }

        if (StringUtils.trimToNull(statement.getTablespace()) != null && database.supportsTablespaces()) {
            if (database instanceof MSSQLDatabase) {
                sql += " ON "+statement.getTablespace();
            } else if (database instanceof DB2Database || database instanceof SybaseASADatabase) {
                ; //not supported
            } else {
                sql += " USING INDEX TABLESPACE \""+statement.getTablespace() + "\"";
            }
        }

        if (statement.getForIndexName() != null) {
            sql += " USING INDEX "+database.escapeObjectName(statement.getForIndexCatalogName(), statement.getForIndexSchemaName(), statement.getForIndexName(), Index.class);
        }

        return new Sql[] {
                new UnparsedSql(sql, getAffectedPrimaryKey(statement))
        };
    }

}
