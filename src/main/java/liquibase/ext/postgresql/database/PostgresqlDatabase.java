package liquibase.ext.postgresql.database;

import liquibase.database.ObjectQuotingStrategy;
import liquibase.database.core.PostgresDatabase;
import liquibase.structure.DatabaseObject;

/**
 * Created by dmd on 4/29/2017.
 */
public class PostgresqlDatabase extends PostgresDatabase {


    @Override
    public int getPriority() {
        return 5;
    }


    public PostgresqlDatabase(){
        this.quotingStrategy =ObjectQuotingStrategy.QUOTE_ALL_OBJECTS;
    }

    @Override
    public void setObjectQuotingStrategy(ObjectQuotingStrategy quotingStrategy) {
        this.quotingStrategy =ObjectQuotingStrategy.QUOTE_ALL_OBJECTS;
    }

    //    @Override
//    public String escapeObjectName(String objectName, final Class<? extends DatabaseObject> objectType) {
//        System.out.println("PostgresqlDatabase.escapeObjectName called.");
//        if (objectName!=null && !objectName.startsWith("\"")) {
//            return "\"" + objectName + "\"";
//        } else {
//            return super.escapeObjectName(objectName, objectType);
//        }
//    }
//
//    @Override
//    protected boolean mustQuoteObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
//        System.out.println("PostgresqlDatabase.mustQuoteObjectName called.");
//        return true;
//    }

    @Override
    public ObjectQuotingStrategy getObjectQuotingStrategy() {
        System.out.println("PostgresqlDatabase.getObjectQuotingStrategy called.");
        return ObjectQuotingStrategy.QUOTE_ALL_OBJECTS;
    }

//    @Override
//    public String correctObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
//        if (objectName!=null && !objectName.startsWith("\"")) {
//            return "\"" + objectName + "\"";
//        } else {
//            return super.correctObjectName(objectName, objectType);
//        }
//    }


//    @Override
//    public String escapeColumnName(String catalogName, String schemaName, String tableName, String columnName) {
//        return this.escapeObjectName(columnName, Column.class);
//    }
//
//    @Override
//    public String escapeTableName(String catalogName, String schemaName, String tableName) {
//        return this.escapeObjectName(tableName, Table.class);
//    }
}
