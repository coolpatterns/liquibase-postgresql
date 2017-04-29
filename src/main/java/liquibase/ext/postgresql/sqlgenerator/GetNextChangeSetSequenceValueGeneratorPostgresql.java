package liquibase.ext.postgresql.sqlgenerator;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.ext.postgresql.database.PostgresqlDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.GetNextChangeSetSequenceValueGenerator;
import liquibase.statement.core.GetNextChangeSetSequenceValueStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;

/**
 * Created by dmd on 4/29/2017.
 */
public class GetNextChangeSetSequenceValueGeneratorPostgresql extends GetNextChangeSetSequenceValueGenerator{


    @Override
    public int getPriority() {
        return 5;
    }

    @Override
    public boolean supports(GetNextChangeSetSequenceValueStatement statement, Database database) {
        return database instanceof PostgresqlDatabase;
    }


    @Override
    public Sql[] generateSql(GetNextChangeSetSequenceValueStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return SqlGeneratorFactory.getInstance().generateSql(new SelectFromDatabaseChangeLogStatement(new ColumnConfig().setName("MAX(\"ORDEREXECUTED\")", true)), database);
    }

}
