package liquibase.ext.postgresql.sqlgenerator;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.postgresql.database.PostgresqlDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SelectFromDatabaseChangeLogGenerator;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;
import liquibase.util.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by dmd on 4/30/2017.
 */
public class SelectFromDatabaseChangeLogGeneratorPostgresql extends SelectFromDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return 5;
    }

    @Override
    public boolean supports(SelectFromDatabaseChangeLogStatement statement, Database database) {
        return database instanceof PostgresqlDatabase;
    }


    @Override
    public Sql[] generateSql(SelectFromDatabaseChangeLogStatement statement, final Database database, SqlGeneratorChain sqlGeneratorChain) {
        List<ColumnConfig> columnsToSelect = Arrays.asList(statement.getColumnsToSelect());
        String sql = "SELECT " + StringUtils.join(columnsToSelect, ",", new StringUtils.StringUtilsFormatter<ColumnConfig>() {
            @Override
            public String toString(ColumnConfig column) {
                if (column.getComputed() != null && column.getComputed()) {
                    return column.getName();
                } else {
                    return database.escapeColumnName(null, null, null, column.getName());
                }
            }
        }).toUpperCase() + " FROM " +
                database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName());

        SelectFromDatabaseChangeLogStatement.WhereClause whereClause = statement.getWhereClause();
        if (whereClause != null) {
            if (whereClause instanceof SelectFromDatabaseChangeLogStatement.ByTag) {
                sql += " WHERE "+database.escapeColumnName(null, null, null, "TAG")+"='" + ((SelectFromDatabaseChangeLogStatement.ByTag) whereClause).getTagName() + "'";
            } else if (whereClause instanceof SelectFromDatabaseChangeLogStatement.ByNotNullCheckSum) {
                sql += " WHERE MD5SUM IS NOT NULL";
            } else {
                throw new UnexpectedLiquibaseException("Unknown where clause type: " + whereClause.getClass().getName());
            }
        }

        if (statement.getOrderByColumns() != null && statement.getOrderByColumns().length > 0) {
            sql += " ORDER BY ";
            String[] orderByCols = statement.getOrderByColumns();
            boolean first = true;
            for (String col : orderByCols) {
                if (!first) {
                    sql +=",";
                }
                if (col.endsWith("ASC") || col.endsWith("asc") || col.endsWith("DESC") || col.endsWith("desc")) {
                    String[] colWithSc = col.split(" ");
                    sql+="\"" + colWithSc[0].toUpperCase() + "\" " + colWithSc[1];
                } else {
                    sql+="\"" + col.toUpperCase() + "\"";
                }

                first = false;
            }
        }

        return new Sql[]{
                new UnparsedSql(sql)
        };
    }

}
