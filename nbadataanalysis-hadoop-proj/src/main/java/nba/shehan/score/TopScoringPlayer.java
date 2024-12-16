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

import java.io.IOException;

public class TopScoringPlayer {
    public static void run(String[] args) throws Exception {
        main(args);
    }
    public static class PlayerScoreMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            
            String playerName = fields[7]; // PLAYER1_NAME
            String scoreStr = fields[24];  // SCORE

            // Check if both playerName and scoreStr are valid
            if (playerName != null && !playerName.isEmpty() && playerName != "Unknown" && scoreStr != null && !scoreStr.isEmpty()) {
                int points = getPoints(fields[24]); // Extract score difference
                
                if (points > 0) {
                    context.write(new Text(playerName), new IntWritable(points));
                }
            }
        }

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
            return 0;
        }
    }

    public static class PlayerScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text topPlayer = new Text();
        private IntWritable maxScore = new IntWritable(0);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalScore = 0;
            for (IntWritable val : values) {
                totalScore += val.get();
            }
            
            // Update top scorer if the current player's score is higher
            if (totalScore > maxScore.get()) {
                topPlayer.set(key);
                maxScore.set(totalScore);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(topPlayer, maxScore);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top scoring player");
        job.setJarByClass(TopScoringPlayer.class);
        job.setMapperClass(PlayerScoreMapper.class);
        job.setCombinerClass(PlayerScoreReducer.class);
        job.setReducerClass(PlayerScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
