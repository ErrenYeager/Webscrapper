import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class QueryHelper {
    private final DBConnector dbConnector;

    public QueryHelper(DBConnector dbConnector) {
        this.dbConnector = dbConnector;
    }

    public void menu() {
        int choice;
        boolean breakStatement = true;
        System.out.println("Which Query? (Any number out of range causes exit of program)");
        while (breakStatement){
            for (int i = 1; i < 20; i++) {
                System.out.println(i + ". Query" + i);
            }
            System.out.print("Your choice: ");
            choice = ScannerWrapper.getInstance().nextInt();
            switch (choice) {
                case 1 -> getQuery1();
                case 2 -> getQuery2();
                case 3 -> getQuery3();
                case 4 -> getQuery4();
                case 5 -> getQuery5();
                case 6 -> getQuery6();
                case 7 -> getQuery7();
                case 8 -> getQuery8();
                case 9 -> getQuery9();
                case 10 -> getQuery10();
                case 11 -> getQuery11();
                case 12 -> getQuery12();
                case 13 -> getQuery13();
                case 14 -> getQuery14();
                case 15 -> getQuery15();
                case 16 -> getQuery16();
                case 17 -> getQuery17();
                case 18 -> getQuery18();
                case 19 -> getQuery19();
                default -> breakStatement = false;
            }
        }
    }

    public void getQuery1() {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE tags contains ? LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    public void getQuery2() {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE solved > ? AND solved < ? LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    public void getQuery3() {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE difficulty > ? AND difficulty < ? LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }
    public void getQuery4() {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE tags contains ? AND difficulty = ? LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("DIFFICULTY: ");
        Integer diff = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,diff,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }
    public void getQuery5() {
        final String selectQuery = "SELECT AVG(solved) AS avg, MAX(solved) AS max, MIN(solved) AS min FROM project_keyspace.problems_by_difficulty WHERE" +
                " tags contains ? AND difficulty = ? LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("DIFFICULTY: ");
        Integer diff = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,diff,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        resultSet.forEach(row -> {
            System.out.println("AVG : " + row.getInt("avg"));
            System.out.println("MAX : " + row.getInt("max"));
            System.out.println("MIN : " + row.getInt("min"));
        });
    }
    public void getQuery6() {
        final String selectQuery = "SELECT id, divide(cast(solved as float),cast(sent as float)) AS Ratio FROM project_keyspace.problems_by_difficulty WHERE tags CONTAINS ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        resultSet.forEach(row -> {
            System.out.println("ID: " + row.getInt("id"));
            System.out.println("RATIO = " + row.getFloat("Ratio"));
            System.out.println("-----------------------------------------");

        });
    }
    public void getQuery7() {
        System.out.print("Do you want to specify a tag? \n 1. YES\n Any Number. NO\nChoice: ");
        int choice = ScannerWrapper.getInstance().nextInt();
        final String selectQuery;
        PreparedStatement preparedStatement;
        BoundStatement boundStatement;
        if(choice == 1) {
            selectQuery = "SELECT AVG(divide(cast(solved as float),cast(sent as float))) AS ratio, website" +
                    " FROM project_keyspace.problems_by_difficulty where website = ? AND tags CONTAINS ? ALLOW FILTERING";
            System.out.print("TAG: ");
            String tag = ScannerWrapper.getInstance().next();
            System.out.print("WEBSITE: ");
            String website = ScannerWrapper.getInstance().next();
            preparedStatement = dbConnector.getSession().prepare(selectQuery);
            boundStatement = preparedStatement.bind(website,tag);
        } else {
            selectQuery = "SELECT AVG(divide(cast(solved as float),cast(sent as float))) AS ratio, website" +
                    " FROM project_keyspace.problems_by_difficulty WHERE website = ? ALLOW FILTERING";
            System.out.print("WEBSITE: ");
            String website = ScannerWrapper.getInstance().next();
            preparedStatement = dbConnector.getSession().prepare(selectQuery);
            boundStatement = preparedStatement.bind(website);
        }
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        resultSet.forEach(row -> {
            System.out.println("***************************");
            System.out.println("WEBSITE: " + row.getString("website"));
            System.out.println("RATIO: "  + row.getFloat("ratio"));
            System.out.println("***************************");
        });
    }
    public void getQuery8() {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE website = ? ALLOW FILTERING";
        System.out.print("WEBSITE: ");
        String website = ScannerWrapper.getInstance().next();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(website);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        HashSet<String> set = new HashSet<>();
        resultSet.forEach(row -> set.addAll(row.getSet("tags",String.class)));
        int index = 0;
        for (String s : set) {
            if(index < limit) {
                System.out.println(index+1 + ". " + s);
            } else {
                break;
            }
            index++;
        }
    }

    public void updateDifficultyInProblems_by_difficulty(int rowDiff,int rowNewDiff, int rowId, String rowWebsite, String rowQ,
                                                                int rowSent,int rowSolved,HashSet<String> rowTags) {
        final String deleteQuery = "DELETE FROM project_keyspace.problems_by_difficulty WHERE id = ? AND website = ? AND difficulty = ?";
        PreparedStatement psDelete = dbConnector.getSession().prepare(deleteQuery);
        BoundStatement bsDelete = psDelete.bind(rowId, rowWebsite, rowDiff);
        dbConnector.getSession().execute(bsDelete);
        final String insertQuery = "INSERT INTO project_keyspace.problems_by_difficulty" +
                "(id, question, tags, difficulty, sent, solved, website) VALUES(?,?,?,?,?,?,?)";
        PreparedStatement psInsert = dbConnector.getSession().prepare(insertQuery);
        BoundStatement bsInsert = psInsert.bind(rowId, rowQ, rowTags, rowNewDiff, rowSent, rowSolved, rowWebsite);
        dbConnector.getSession().execute(bsInsert);
    }

    public void getQuery9() {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE id = ? ALLOW FILTERING";
        System.out.print("ID: ");
        Integer id = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(id);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        final int[] rowDiff = new int[1];
        final int[] rowSent = new int[1];
        final int[] rowSolved = new int[1];
        final String[] rowWebsite = new String[1];
        final String[] rowQ = new String[1];
        ArrayList<HashSet<String>> rowTags = new ArrayList<>();
        resultSet.forEach(row -> {
            rowDiff[0] = row.getInt("difficulty");
            rowSent[0] = row.getInt("sent");
            rowSolved[0] = row.getInt("solved");
            rowWebsite[0] = row.getString("website");
            rowQ[0] = row.getString("question");
            rowTags.add((HashSet<String>) row.getSet("tags",String.class));
        });
        final String updateQuery2 = "UPDATE project_keyspace.problems_by_sent SET difficulty = ? WHERE id = ? AND website = ? AND sent = ?";
        final String updateQuery3 = "UPDATE project_keyspace.problems_by_solved SET difficulty = ? WHERE id = ? AND website = ? AND solved = ?";
        System.out.print("Set DIFFICULTY to: ");
        Integer newDiff = ScannerWrapper.getInstance().nextInt();
        updateDifficultyInProblems_by_difficulty(rowDiff[0],newDiff,id,rowWebsite[0],rowQ[0],rowSent[0],rowSolved[0],rowTags.get(0));
        PreparedStatement psUpdate2 = dbConnector.getSession().prepare(updateQuery2);
        BoundStatement bsUpdate2 = psUpdate2.bind(newDiff,id, rowWebsite[0], rowSent[0]);
        dbConnector.getSession().execute(bsUpdate2);
        PreparedStatement psUpdate3 = dbConnector.getSession().prepare(updateQuery3);
        BoundStatement bsUpdate3 = psUpdate3.bind(newDiff,id,rowWebsite[0],rowSolved[0]);
        dbConnector.getSession().execute(bsUpdate3);
        System.out.println("Updated successfully!");
    }
    public void getQuery10() {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE tags contains ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        System.out.print("Set DIFFICULTY to: ");
        Integer newDiff = ScannerWrapper.getInstance().nextInt();
        resultSet.forEach(row -> {
            final String updateQuery2 = "UPDATE project_keyspace.problems_by_sent SET difficulty = ? WHERE id = ? AND website = ? AND sent = ?";
            final String updateQuery3 = "UPDATE project_keyspace.problems_by_solved SET difficulty = ? WHERE id = ? AND website = ? AND solved = ?";
            updateDifficultyInProblems_by_difficulty(row.getInt("difficulty"),newDiff,row.getInt("id"),row.getString("website"),
                    row.getString("question"),row.getInt("sent"),row.getInt("solved"),(HashSet<String>)row.getSet("tags",String.class));
            PreparedStatement psUpdate2 = dbConnector.getSession().prepare(updateQuery2);
            BoundStatement bsUpdate2 = psUpdate2.bind(newDiff,row.getInt("id"), row.getString("website"), row.getInt("sent"));
            dbConnector.getSession().execute(bsUpdate2);
            PreparedStatement psUpdate3 = dbConnector.getSession().prepare(updateQuery3);
            BoundStatement bsUpdate3 = psUpdate3.bind(newDiff,row.getInt("id"), row.getString("website"), row.getInt("solved"));
            dbConnector.getSession().execute(bsUpdate3);
        });
        System.out.println("Updated successfully!");
    }
    public void getQuery11() {
        final String deleteQuery1 = "DELETE FROM project_keyspace.problems_by_difficulty WHERE id IN " + idMaker() +" AND website = ?";
        final String deleteQuery2 = "DELETE FROM project_keyspace.problems_by_sent WHERE id IN " + idMaker() +" AND website = ?";
        final String deleteQuery3 = "DELETE FROM project_keyspace.problems_by_solved WHERE id IN " + idMaker() +" AND website = ?";
        System.out.print("WEBSITE: ");
        String rowWebsite = ScannerWrapper.getInstance().next();
        PreparedStatement psDelete1 = dbConnector.getSession().prepare(deleteQuery1);
        BoundStatement bsDelete1 = psDelete1.bind(rowWebsite);
        dbConnector.getSession().execute(bsDelete1);
        PreparedStatement psDelete2 = dbConnector.getSession().prepare(deleteQuery2);
        BoundStatement bsDelete2 = psDelete2.bind(rowWebsite);
        dbConnector.getSession().execute(bsDelete2);
        PreparedStatement psDelete3 = dbConnector.getSession().prepare(deleteQuery3);
        BoundStatement bsDelete3 = psDelete3.bind(rowWebsite);
        dbConnector.getSession().execute(bsDelete3);
        System.out.println("Deleted Successfully!");
    }

    private int query12Helper() {
        int choice = 100;
        System.out.println("1. Query 1 (ASC)");
        System.out.println("2. Query 1 (DESC)");
        System.out.println("3. Query 2 (ASC)");
        System.out.println("4. Query 2 (DESC)");
        System.out.println("5. Query 3 (ASC)");
        System.out.println("6. Query 3 (DESC)");
        while (choice < 1 || choice > 6) {
            System.out.print("Choose One: ");
            choice = ScannerWrapper.getInstance().nextInt();
        }
        return choice;
    }

    public void getQuery12() {
        int choice = query12Helper();
        switch (choice) {
            case 1 -> query12_1(dbConnector, "ASC");
            case 2 -> query12_1(dbConnector, "DESC");
            case 3 -> query12_2(dbConnector, "ASC");
            case 4 -> query12_2(dbConnector, "DESC");
            case 5 -> query12_3(dbConnector, "ASC");
            default -> query12_3(dbConnector, "DESC");
        }
    }

    public void query12_1(DBConnector dbConnector, String sorting) {
        //todo: websites
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE tags contains ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY difficulty " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }
    public void query12_2(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE solved > ? AND solved < ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY difficulty " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }
    public void query12_3(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_difficulty WHERE difficulty > ? AND difficulty < ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY difficulty " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    public int query13Helper() {
        int choice = 100;
        System.out.println("1. Query 1 (ASC)");
        System.out.println("2. Query 1 (DESC)");
        System.out.println("3. Query 2 (ASC)");
        System.out.println("4. Query 2 (DESC)");
        System.out.println("5. Query 3 (ASC)");
        System.out.println("6. Query 3 (DESC)");
        System.out.println("7. Query 4 (ASC)");
        System.out.println("8. Query 4 (DESC)");
        while (choice < 1 || choice > 6) {
            System.out.print("Choose One: ");
            choice = ScannerWrapper.getInstance().nextInt();
        }
        return choice;
    }

    public void getQuery13() {
        int choice = query13Helper();
        switch (choice) {
            case 1 -> query13_1(dbConnector, "ASC");
            case 2 -> query13_1(dbConnector, "DESC");
            case 3 -> query13_2(dbConnector, "ASC");
            case 4 -> query13_2(dbConnector, "DESC");
            case 5 -> query13_3(dbConnector, "ASC");
            case 6 -> query13_3(dbConnector, "DESC");
            case 7 -> query13_4(dbConnector, "ASC");
            default -> query13_4(dbConnector, "DESC");
        }
    }

    private void query13_1(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_sent WHERE tags contains ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY sent " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    private void query13_2(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_sent WHERE solved > ? AND solved < ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY sent " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    private void query13_3(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_sent WHERE difficulty > ? AND difficulty < ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY sent " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    private void query13_4(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_sent WHERE tags contains ? AND difficulty = ? id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY sent " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("DIFFICULTY: ");
        Integer diff = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,diff,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    public void getQuery14() {
        int choice = query13Helper();
        switch (choice) {
            case 1 -> query14_1(dbConnector, "ASC");
            case 2 -> query14_1(dbConnector, "DESC");
            case 3 -> query14_2(dbConnector, "ASC");
            case 4 -> query14_2(dbConnector, "DESC");
            case 5 -> query14_3(dbConnector, "ASC");
            case 6 -> query14_3(dbConnector, "DESC");
            case 7 -> query14_4(dbConnector, "ASC");
            default -> query14_4(dbConnector, "DESC");
        }
    }

    private void query14_1(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_solved WHERE tags contains ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY solved " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    private void query14_2(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_solved WHERE solved > ? AND solved < ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY solved " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    private void query14_3(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_solved WHERE difficulty > ? AND difficulty < ? AND id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY solved " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("lower bound: ");
        Integer lower = ScannerWrapper.getInstance().nextInt();
        System.out.print("higher bound: ");
        Integer higher = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(lower,higher,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    private void query14_4(DBConnector dbConnector, String sorting) {
        final String selectQuery = "SELECT * FROM project_keyspace.problems_by_solved WHERE tags contains ? AND difficulty = ? id IN " +
                idMaker() + " AND website IN ('CodeForces','LeetCode') ORDER BY solved " + sorting + " LIMIT ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        System.out.print("DIFFICULTY: ");
        Integer diff = ScannerWrapper.getInstance().nextInt();
        System.out.print("LIMIT: ");
        Integer limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag,diff,limit);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    public void getQuery15() {
        final String selectQuery = "SELECT MIN(difficulty) AS min, MAX(difficulty) AS max FROM project_keyspace.problems_by_difficulty WHERE tags contains ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        resultSet.forEach(row -> {
            System.out.println("Min difficulty : " + row.getInt("min"));
            System.out.println("Max difficulty : " + row.getInt("max"));
        });
    }

    public void getQuery16() {
        final String selectQuery = "SELECT AVG(difficulty) AS avg FROM project_keyspace.problems_by_difficulty WHERE tags contains ? ALLOW FILTERING";
        System.out.print("TAG: ");
        String tag = ScannerWrapper.getInstance().next();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        resultSet.forEach(row -> System.out.println("AVG of difficulty : " + row.getInt("avg")));
    }

    public void getQuery17() {
        final String selectQuery ="SELECT AVG(difficulty) AS avg FROM project_keyspace.problems_by_difficulty WHERE website = ? ALLOW FILTERING";
        System.out.print("WEBSITE: ");
        String tag = ScannerWrapper.getInstance().next();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tag);
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        resultSet.forEach(row -> System.out.println("AVG of difficulty : " + row.getInt("avg")));
    }

    public void prepareQuery18(DBConnector dbConnector) {
        final String createQuery = "CREATE TABLE IF NOT EXISTS project_keyspace.tag_ratio" +
                "(tag varchar, ratio float, PRIMARY KEY (tag, ratio))";
        dbConnector.getSession().execute(createQuery);
        PreparedStatement preparedStatement;
        BoundStatement boundStatement;
        ResultSet resultSet;
        HashMap<String, Float> tagRatioMap = new HashMap<>();
        HashSet<String> setOfTags = getTagSet(dbConnector);
        for (String tag : setOfTags) {
            final String selectQuery = "SELECT SUM(divide(cast(solved as float),cast(sent as float))) AS RatioSUM FROM " +
                    "project_keyspace.problems_by_difficulty WHERE tags CONTAINS ? ALLOW FILTERING";
            preparedStatement = dbConnector.getSession().prepare(selectQuery);
            boundStatement = preparedStatement.bind(tag);
            resultSet = dbConnector.getSession().execute(boundStatement);
            resultSet.forEach(row -> tagRatioMap.put(tag, row.getFloat("RatioSum")));
        }
        final String insertQuery = "INSERT INTO project_keyspace.tag_ratio (tag, ratio)" +
                "VALUES (?,?)";
        for (Map.Entry<String, Float> set : tagRatioMap.entrySet()) {
            PreparedStatement psInsert = dbConnector.getSession().prepare(insertQuery);
            BoundStatement bsInsert = psInsert.bind(set.getKey(),set.getValue());
            dbConnector.getSession().execute(bsInsert);
        }
    }

    public void getQuery18() {
        prepareQuery18(dbConnector);
        System.out.println("Sorting Method");
        int sortingMethod = 100;
        while (sortingMethod > 2 || sortingMethod < 1) {
            System.out.println("1. ASC");
            System.out.println("2. DESC");
            System.out.print("Your Choice: ");
            sortingMethod = ScannerWrapper.getInstance().nextInt();
        }
        String sorting;
        if (sortingMethod == 1) {
            sorting = "ASC";
        } else {
            sorting = "DESC";
        }
        final String finalSelectQuery = "SELECT * FROM project_keyspace.tag_ratio WHERE tag IN " + tagMaker(getTagSet(dbConnector)) + " ORDER BY ratio "+ sorting + " LIMIT ?";
        System.out.print("LIMIT: ");
        int limit = ScannerWrapper.getInstance().nextInt();
        PreparedStatement finalPreparedStatement = dbConnector.getSession().prepare(finalSelectQuery);
        BoundStatement finalBoundStatement = finalPreparedStatement.bind(limit);
        ResultSet finalResultSet = dbConnector.getSession().execute(finalBoundStatement);
        finalResultSet.forEach(row -> {
            System.out.println("tag: " + row.getString("tag"));
            System.out.println("ratio: " + row.getFloat("ratio"));
            System.out.println("-------------------------------------");
        });
    }

    public void getQuery19() {
        String IDs = idMaker();
        final String selectQuery ="SELECT * FROM project_keyspace.problems_by_difficulty WHERE id IN " + IDs +
                "AND tags CONTAINS ? AND website IN ?  ALLOW FILTERING";
        System.out.print("TAG: ");
        String tags = ScannerWrapper.getInstance().next();
        System.out.print("WEBSITES (Separate with comma): ");
        String websites = ScannerWrapper.getInstance().next();
        PreparedStatement preparedStatement = dbConnector.getSession().prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(tags, setMaker(websites));
        ResultSet resultSet = dbConnector.getSession().execute(boundStatement);
        selectStar(resultSet);
    }

    public void selectStar(ResultSet resultSet) {
        resultSet.forEach(row -> {
            System.out.println("id : " + row.getInt("id"));
            System.out.println("question : " + row.getString("question"));
            System.out.println("tags : " + row.getSet("tags",String.class));
            System.out.println("difficulty : " + row.getInt("difficulty"));
            System.out.println("sent : " + row.getInt("sent"));
            System.out.println("solved : " + row.getInt("solved"));
            System.out.println("website : " + row.getString("website"));
            System.out.println("---------------------------------------------------------------------------------------");
        });
    }

    public String idMaker() {
        StringBuilder IDs = new StringBuilder("(");
        for (int i = 1; i <= 150; i++) {
            IDs.append(i);
            if(i != 150) {
                IDs.append(", ");
            } else {
                IDs.append(")");
            }
        }
        return String.valueOf(IDs);
    }

    private ArrayList<String> setMaker(String input) {
        ArrayList<String> arrayList = new ArrayList<>();
        String[] array = input.split(",");
        for(int i = 0; i < array.length; i++) {
            arrayList.add(array[0]);
        }
        return arrayList;
    }

    private HashSet<String> getTagSet(DBConnector dbConnector) {
        final String selectQuery1 = "SELECT * FROM project_keyspace.problems_by_difficulty";
        PreparedStatement preparedStatement2 = dbConnector.getSession().prepare(selectQuery1);
        BoundStatement boundStatement2 = preparedStatement2.bind();
        ResultSet resultSet2 = dbConnector.getSession().execute(boundStatement2);
        HashSet<String> setOfTags = new HashSet<>();
        resultSet2.forEach(row -> setOfTags.addAll(row.getSet("tags",String.class)));
        return setOfTags;
    }

    private String tagMaker(HashSet<String> tagSet) {
        StringBuilder tagString = new StringBuilder();
        int index = 1;
        for (String tag : tagSet) {
            if(index == tagSet.size()) {
                tagString.append("'").append(tag).append("'");
            } else {
                tagString.append("'").append(tag).append("'").append(",");
            }
            index++;
        }
        return "(" + tagString + ")";
    }
}
