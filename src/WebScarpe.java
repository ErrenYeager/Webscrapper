
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.Arrays;
import java.util.HashSet;

public class WebScarpe {

    private ProblemTable problems;

    public WebScarpe(ProblemTable problems) {
        this.problems = problems;
    }

    public Integer[] difAndSolvedCodeforcesData(String data) {
        String[] splitByX = data.split("x");
        String solved = splitByX[splitByX.length-1];
        String[] splitByXBySpace  = splitByX[splitByX.length - 2].split("\\s+");
        String difficulty;
        Integer[] difAndSolved = new Integer[2];
        if (splitByXBySpace[splitByXBySpace.length - 1].matches("\\d+")) {
            difficulty = splitByXBySpace[splitByXBySpace.length - 1];
            difAndSolved[0] = Integer.parseInt(difficulty);
        } else {
            difAndSolved[0] = null;
        }
        difAndSolved[1] = Integer.parseInt(solved);
        return difAndSolved;
    }
    public String titleCodeforces(String idAndTitleAndSolved) {
        String[] splitByX = idAndTitleAndSolved.split("x");
        String solved = splitByX[splitByX.length-1];
        return idAndTitleAndSolved.replaceAll("x"+solved,"");
    }

    public HashSet<String> prepareTags(String tags) {
        String[] tagsArray = tags.split("\\s+");
        return new HashSet<>(Arrays.asList(tagsArray));
    }

    public void crawlCodeforces() {
        final String codeForcesUrl = "https://codeforces.com/problemset";
        try {
            final Document codeForcesDocument = Jsoup.connect(codeForcesUrl).get();
            Elements codeForcesElements =  codeForcesDocument.select("body div#body div div#pageContent.content-with-sidebar div.datatable div table.problems tbody tr");
            for (Element e : codeForcesElements) {
                String data = e.select("td").text();
                String tags = e.select("a.notice").text();
                String idAndTitleAndSolved = e.select("a").text().replaceAll(tags, "");
                if (!data.equals("")) {
                    Integer[] difAndSolved = difAndSolvedCodeforcesData(data);
                    Record newRecord = new Record(titleCodeforces(idAndTitleAndSolved).trim(), prepareTags(tags), difAndSolved[0], difAndSolved[1], "CodeForces");
                    problems.addRecord(newRecord);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("*** CodeForces scraped successfully! ***");
    }

    public String[] cleanLeetCodeInfo(String information, int index) {
        String title = "";
        String acceptance;
        String difficulty;
        if (information.split("\\.")[0].equals(String.valueOf(index))) {
            String[] tableRowArray = information.split("\\s+");
            int length = 0;
            for (int i = 0; i < tableRowArray.length; i++) {
                if (tableRowArray[i].equals("Hard") || tableRowArray[i].equals("Medium") || tableRowArray[i].equals("Easy")) {
                    length = i;
                    break;
                }
            }
            for (int i = 1; i < length - 1; i++) {
                title = title + " " + tableRowArray[i];
            }
            acceptance = tableRowArray[length-1];
            difficulty = tableRowArray[length];
            return new String[]{title, acceptance, difficulty};
        }
        return null;
    }

    public void crawlLeetCode() {
        final String leetCodeUrl = "https://leetcode.com/problemset/all/?page=1";
        try {
            final Document leetCodeDocument = Jsoup.connect(leetCodeUrl).get();
            Elements leetCodeElements =  leetCodeDocument.select("div.inline-block.min-w-full div");
            int index = 1;
            for (Element e : leetCodeElements) {
                if(! e.select("div").text().equals("")) {
                    String[] cleanedData = cleanLeetCodeInfo(e.select("div").text(), index);
                    if(cleanedData != null) {
                        Record newRecord = new Record(cleanedData[0],cleanedData[1],cleanedData[2], "LeetCode");
                        problems.addRecord(newRecord);
                        index++;

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("*** LeetCode scraped successfully! ***");
    }

}