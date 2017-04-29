package liquibase.ext.postgresql.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.postgresql.database.PostgresqlDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateIndexGeneratorPostgres;
import liquibase.statement.core.CreateIndexStatement;

/**
 * Created by dmd on 4/30/2017.
 */
public class CreateIndexGeneratorPostgresql extends CreateIndexGeneratorPostgres {

    @Override
    public int getPriority() {
        return 10;
    }


    @Override
    public boolean supports(CreateIndexStatement statement, Database database) {
        return database instanceof PostgresqlDatabase;
    }

    @Override
    public Sql[] generateSql(CreateIndexStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        statement.setTablespace("\"" + statement.getTablespace() + "\"" );
        return super.generateSql(statement, database, sqlGeneratorChain);
    }
}
