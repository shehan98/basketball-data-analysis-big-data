package nba.shehan;

import nba.shehan.preprocessing.DataCleaningJob;
import nba.shehan.score.MostScoreQuarterByTeam;
import nba.shehan.score.TopScoringPlayer;

public class App 
{
    public static void main(String[] args) 
    {
        if (args.length < 1) {
            System.err.println("Usage: java -jar nbadataanalysis.jar <job-name> [job-parameters]");
            System.err.println("Available jobs:");
            System.err.println("  clean-data               : Run the data cleaning job");
            System.err.println("  most-score-quarter       : Find the team with the highest-scoring quarter");
            System.err.println("  top-scoring-player       : Identify the top-scoring player");
            System.exit(-1);
        }

        String jobName = args[0];

        try {
            switch (jobName) {
                case "clean-data":
                    System.out.println("Starting Data Cleaning Job...");
                    DataCleaningJob.run(args);
                    break;

                case "most-score-quarter":
                    System.out.println("Starting Most Score Quarter Analysis...");
                    MostScoreQuarterByTeam.run(args);
                    break;

                case "top-scoring-player":
                    System.out.println("Starting Top Scoring Player Analysis...");
                    TopScoringPlayer.run(args);
                    break;

                default:
                    System.err.println("Invalid job name: " + jobName);
                    System.err.println("Available jobs: clean-data, most-score-quarter, top-scoring-player");
                    System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Job failed due to an error: " + e.getMessage());
            System.exit(-1);
        }
    }
}