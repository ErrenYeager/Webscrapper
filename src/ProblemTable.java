import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import java.util.ArrayList;

public class ProblemTable {
    private String tableName;
    private final ArrayList<Record> records = new ArrayList<>();

    public ProblemTable(String tableName) {
        this.tableName = tableName;
    }
    public void addRecord(Record record) {
        this.records.add(record);
    }

    public ArrayList<Record> getRecords() {
        return records;
    }

    public void createKeyspace(DBConnector dbConnector) {
        final String createProjectKeySpace = "CREATE KEYSPACE IF NOT EXISTS project_keyspace WITH" +
                " replication = {'class' : 'SimpleStrategy','replication_factor' :3}";
        dbConnector.getSession().execute(createProjectKeySpace);
        System.err.println("*** KeySpace Created ***");
        final String createFunction = """
                CREATE FUNCTION IF NOT EXISTS project_keyspace.Divide(x float,y float)
                       CALLED ON NULL INPUT
                       RETURNS float
                       LANGUAGE java
                       AS $$return x/y;$$;
                """;
        dbConnector.getSession().execute(createFunction);
    }

    public void createTablesNeeded(DBConnector dbConnector) {
        final String createStatement1 = "CREATE TABLE IF NOT EXISTS project_keyspace.problems_by_difficulty"
                +"(id int, question varchar, tags set<varchar>, difficulty int, sent int, solved int, website varchar" +
                ", PRIMARY KEY ((id, website), difficulty))";
        dbConnector.getSession().execute(createStatement1);
        final String createStatement2 = "CREATE TABLE IF NOT EXISTS project_keyspace.problems_by_sent"
                +"(id int, question varchar, tags set<varchar>, difficulty int, sent int, solved int, website varchar" +
                ", PRIMARY KEY ((id, website), sent))";
        dbConnector.getSession().execute(createStatement2);
        final String createStatement3 = "CREATE TABLE IF NOT EXISTS project_keyspace.problems_by_solved"
                +"(id int, question varchar, tags set<varchar>, difficulty int, sent int, solved int, website varchar" +
                ", PRIMARY KEY ((id, website), solved))";
        dbConnector.getSession().execute(createStatement3);
        System.err.println("*** Tables Created ***");
    }

    public void insertTable(DBConnector dbConnector, String insertQuery) {
        for (int i = 0; i < this.getRecords().size(); i++) {
            PreparedStatement psInsert = dbConnector.getSession().prepare(insertQuery);
            BoundStatement bsInsert = psInsert.bind(this.getRecords().get(i).getId(), this.getRecords().get(i).getQuestionTitle()
                    , this.getRecords().get(i).getQuestionTags()
                    ,this.getRecords().get(i).getDifficultyDegree(), this.getRecords().get(i).getCountOfSentSolves()
                    , this.getRecords().get(i).getCountOfAcceptedSolves(), this.getRecords().get(i).getWebsite());
            dbConnector.getSession().execute(bsInsert);
        }
    }

}
