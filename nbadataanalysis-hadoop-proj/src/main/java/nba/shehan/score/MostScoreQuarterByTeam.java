package nba.shehan.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MostScoreQuarterByTeam {
    public static void run(String[] args) throws Exception {
        main(args);
    }

    public static class MostScoringQuarterMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        // Map method to extract team, period, and score
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip this record if incomplete
            if (fields.length < 24) {
                return;
            }
            
            try {
                String team = fields[11]; // PLAYER1_TEAM_NICKNAME
                int period = Integer.parseInt(fields[5]); // PERIOD
                String scoreStr = fields[24]; // SCORE

                if (team.isEmpty() || period <= 0 || period > 4 || scoreStr.isEmpty()) {
                    return; // Skip invalid records
                }

                int points = getPoints(scoreStr);
                
                // Emit key-value pairs: Key: "TEAM_PERIOD", Value: points
                context.write(new Text(team + "_Q" + period), new IntWritable(points));
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                System.err.println("Invalid record: " + line);
            }
        }

        // Method to calculate points scored based on score difference
        private int getPoints(String scoreStr) {
            String[] scores = scoreStr.trim().replaceAll("\\s", "").split("-");
            if (scores.length == 2) {
                try {
                    int currentScore = Integer.parseInt(scores[0].trim());
                    int previousScore = Integer.parseInt(scores[1].trim());
                    return Math.abs(currentScore - previousScore);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid score format: " + scoreStr);
                    return 0;
                }
            }
            System.err.println("Invalid score format: " + scoreStr);
            return 0; // If score format is invalid or incomplete
        }
    }

    // Reducer class to aggregate the total points per team per quarter
    public static class MostScoringQuarterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Map to store the total points for each team and quarter
        private Map<String, Map<String, Integer>> teamQuarterScores = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalPoints = 0;
            for (IntWritable val : values) {
                totalPoints += val.get();
            }

            // Split the key to separate the team and quarter
            String[] keyParts = key.toString().split("_Q");
            if (keyParts.length != 2) {
                return;
            }

            String team = keyParts[0];
            String quarter = keyParts[1];

            // Update the team's quarter score map
            teamQuarterScores.putIfAbsent(team, new HashMap<>());
            Map<String, Integer> quarterScores = teamQuarterScores.get(team);
            quarterScores.put(quarter, quarterScores.getOrDefault(quarter, 0) + totalPoints);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Find the most scored quarter for each team
            for (Map.Entry<String, Map<String, Integer>> entry : teamQuarterScores.entrySet()) {
                String team = entry.getKey();
                Map<String, Integer> quarterScores = entry.getValue();

                String highestQuarter = null;
                int highestScore = 0;

                // Iterate over all quarters to find the one with the highest score
                for (Map.Entry<String, Integer> quarterEntry : quarterScores.entrySet()) {
                    String quarter = quarterEntry.getKey();
                    int score = quarterEntry.getValue();

                    if (score > highestScore) {
                        highestScore = score;
                        highestQuarter = quarter;
                    }
                }

                if (highestQuarter != null && highestScore > 0) {
                    context.write(new Text(team + "_Q" + highestQuarter), new IntWritable(highestScore));
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MostScoreQuarterByTeam <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Most Scoring Quarter By Team");
        job.setJarByClass(MostScoreQuarterByTeam.class);
        job.setMapperClass(MostScoringQuarterMapper.class);
        job.setReducerClass(MostScoringQuarterReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}