public class Main {

    public static void main(String[] args) {
        ProblemTable table = new ProblemTable("problems");
        WebScarpe webScarpe = new WebScarpe(table);
        webScarpe.crawlCodeforces();
        webScarpe.crawlLeetCode();
        try {
            DBConnector dbConnector = new DBConnector();
            dbConnector.connectDB("localhost", 9042);
            table.createKeyspace(dbConnector);
            table.createTablesNeeded(dbConnector);
            final String insertQuery1 = "INSERT INTO project_keyspace.problems_by_difficulty(id, question, tags, difficulty, sent, solved, website)" +
                    " VALUES(?,?,?,?,?,?,?)";
            table.insertTable(dbConnector,insertQuery1);
            final String insertQuery2 = "INSERT INTO project_keyspace.problems_by_sent(id, question, tags, difficulty, sent, solved, website)" +
                    " VALUES(?,?,?,?,?,?,?)";
            table.insertTable(dbConnector,insertQuery2);
            final String insertQuery3 = "INSERT INTO project_keyspace.problems_by_solved(id, question, tags, difficulty, sent, solved, website)" +
                    " VALUES(?,?,?,?,?,?,?)";
            table.insertTable(dbConnector,insertQuery3);
            System.err.println("*** Tables Inserted ***");
            QueryHelper queryHelper = new QueryHelper(dbConnector);
            queryHelper.menu();
            ScannerWrapper.getInstance().close();
            dbConnector.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
