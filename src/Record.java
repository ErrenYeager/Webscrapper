import java.util.HashSet;
import java.util.Set;

public class Record {
    private int id;
    private String questionTitle;
    private final HashSet<String> questionTags = new HashSet<>();
    private Integer difficultyDegree;
    private Integer countOfSentSolves;
    private Integer countOfAcceptedSolves;
    private final String website;

    public Record(String questionTitle, HashSet<String> questionTags, Integer difficultyDegree, Integer countOfSolved, String website) {
        this.website = website;
        this.questionTitle = questionTitle;
        this.questionTags.addAll(questionTags);
        setDifficultyDegree(difficultyDegree);
        this.countOfAcceptedSolves = countOfSolved;
        this.countOfSentSolves = countOfSolved + RandomHelper.randomInRange(500,5000);
        this.id = IDGenerator.nextID();
    }

    public Record(String questionTitle, String acceptance, String difficultyDegree,String website) {
        this.website = website;
        this.questionTitle = questionTitle;
        this.difficultyDegree = convertStringToNumber(difficultyDegree);
        this.countOfAcceptedSolves = convertPercentToCount(acceptance);
        this.id = IDGenerator.nextID();
    }



    public int getId() {
        return id;
    }

    public int convertPercentToCount(String acceptance) {
        String percent = acceptance.replace("%", "");
        this.countOfSentSolves = RandomHelper.randomInRange(500,20000);
        return (int)((double) countOfSentSolves * (Double.parseDouble(percent) / 100));
    }

    public void addTag(String newTag) {
        questionTags.add(newTag);
    }

    private int convertStringToNumber(String difficulty) {
        if (difficulty.equals("Hard")) {
            return RandomHelper.randomInRange(2200, 4000);
        } else if (difficulty.equals("Medium")) {
            return RandomHelper.randomInRange(1500, 2200);
        } else {
            return RandomHelper.randomInRange(500, 1500);
        }
    }

    public void setDifficultyDegree(Integer difficultyDegree) {
        if (difficultyDegree != null) {
            this.difficultyDegree = difficultyDegree;
        } else {
            this.difficultyDegree = 0;
        }
    }

    public String getWebsite() {
        return website;
    }

    public String getQuestionTitle() {
        return questionTitle;
    }

    public Set<String> getQuestionTags() {
        return questionTags;
    }

    public Integer getDifficultyDegree() {
        return difficultyDegree;
    }

    public Integer getCountOfSentSolves() {
        return countOfSentSolves;
    }

    public Integer getCountOfAcceptedSolves() {
        return countOfAcceptedSolves;
    }
}
