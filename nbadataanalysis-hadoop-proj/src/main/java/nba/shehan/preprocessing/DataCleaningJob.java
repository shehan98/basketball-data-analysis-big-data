package nba.shehan.preprocessing;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataCleaningJob {
    public static void run(String[] args) throws Exception {
        main(args);
    }
    public static class DataCleaningMapper extends Mapper<Object, Text, Text, Text> {

        private int rowCounter = 0;

        private static final String DEFAULT_EVENT_ID = "-1";
        private static final String DEFAULT_EVENT_NUM = "-1";
        private static final String DEFAULT_GAME_ID = "-1";
        private static final String DEFAULT_DESCRIPTION = "Unknown";
        private static final String DEFAULT_TIME = "00:00";
        private static final String DEFAULT_PERIOD = "0";
        private static final String DEFAULT_PLAYER_ID = "-1";
        private static final String DEFAULT_TEAM_ABBR = "NA";
        private static final String DEFAULT_TEAM_CITY = "Unknown";
        private static final String DEFAULT_TEAM_ID = "-1";
        private static final String DEFAULT_TEAM_NAME = "Unknown";
        private static final String DEFAULT_MARGIN = "0";
        private static final String DEFAULT_DESCRIPTION_EMT = "";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            rowCounter++;

            if (rowCounter <= 2) {
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",", -1);

            // Check if player1Id and player1TeamId are invalid
            String player1Id = fields[6];
            String player1TeamId = fields[10];
            if (!validateInteger(player1Id) || "0".equals(player1Id) || !validateEmptyString(player1TeamId) || "0".equals(player1TeamId)) {
                // Skip this record entirely
                return;
            }

            // Validate and clean each field
            String eventId = validateInteger(fields[0]) ? fields[0] : DEFAULT_EVENT_ID;
            String eventNum = validateInteger(fields[1]) ? fields[1] : DEFAULT_EVENT_NUM;
            String gameId = validateInteger(fields[2]) ? fields[2] : DEFAULT_GAME_ID;
            String homeDescription = validateString(fields[3]) ? fields[3] : DEFAULT_DESCRIPTION_EMT;
            String time = validateTimeFormat(fields[4]) ? fields[4] : DEFAULT_TIME;
            String period = validateInteger(fields[5]) ? fields[5] : DEFAULT_PERIOD;
            String player1Name = validateString(fields[7]) ? fields[7] : DEFAULT_DESCRIPTION;
            String player1TeamAbbr = validateString(fields[8]) ? fields[8] : DEFAULT_TEAM_ABBR;
            String player1TeamCity = validateString(fields[9]) ? fields[9] : DEFAULT_TEAM_CITY;
            String player1TeamName = validateString(fields[11]) ? fields[11] : DEFAULT_TEAM_NAME;
            
            String player2Id = validateInteger(fields[12]) ? fields[12] : DEFAULT_PLAYER_ID;
            String player2Name = validateString(fields[13]) ? fields[13] : DEFAULT_DESCRIPTION;
            String player2TeamAbbr = validateString(fields[14]) ? fields[14] : DEFAULT_TEAM_ABBR;
            String player2TeamCity = validateString(fields[15]) ? fields[15] : DEFAULT_TEAM_CITY;
            String player2TeamId = validateEmptyString(fields[16]) ? fields[16] : DEFAULT_TEAM_ID;
            String player2TeamName = validateString(fields[17]) ? fields[17] : DEFAULT_TEAM_NAME;
            
            String player3Id = validateInteger(fields[18]) ? fields[18] : DEFAULT_PLAYER_ID;
            String player3Name = validateString(fields[19]) ? fields[19] : DEFAULT_DESCRIPTION;
            String player3TeamAbbr = validateString(fields[20]) ? fields[20] : DEFAULT_TEAM_ABBR;
            String player3TeamCity = validateString(fields[21]) ? fields[21] : DEFAULT_TEAM_CITY;
            String player3TeamId = validateEmptyString(fields[22]) ? fields[22] : DEFAULT_TEAM_ID;
            String player3TeamName = validateString(fields[23]) ? fields[23] : DEFAULT_TEAM_NAME;

            String rawScore = fields[24];
            String cleanedScore = cleanScore(rawScore);
            if (cleanedScore == null) {
                return;
            }

            String scoreMargin = validateInteger(fields[25]) ? fields[25] : DEFAULT_MARGIN;
            String visitorDescription = validateString(fields[26]) ? fields[26] : DEFAULT_DESCRIPTION_EMT;

            String cleanedRecord = String.join(",",
                eventId, eventNum, gameId, homeDescription, time, period,
                player1Id, player1Name, player1TeamAbbr, player1TeamCity, player1TeamId, player1TeamName,
                player2Id, player2Name, player2TeamAbbr, player2TeamCity, player2TeamId, player2TeamName,
                player3Id, player3Name, player3TeamAbbr, player3TeamCity, player3TeamId, player3TeamName,
                cleanedScore, scoreMargin, visitorDescription
            );

            // Write the cleaned record to the context
            context.write(new Text(), new Text(cleanedRecord));
        }

        private boolean validateInteger(String value) {
            try {
                Integer.parseInt(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        private boolean validateTimeFormat(String value) {
            return value.matches("\\d{2}:\\d{2}");  // Validates mm:ss format
        }

        // Helper method to validate string fields (non-empty)
        private boolean validateString(String value) {
            return value != null && !value.trim().isEmpty();
        }

        private String cleanScore(String score) {
            if (score == null || score.trim().isEmpty()) {
                return null;
            }

            score = score.replaceAll("\\s*-\\s*", "-"); // Normalize spaces around '-'
        
            if (score.matches("\\d+-\\d+")) {
                // X-Y format => X - Y
                String[] parts = score.split("-");
                return parts[0] + " - " + parts[1];
            } else if (score.matches("-\\d+")) {
                // -Y format => 0 - Y
                return "0 - " + score.substring(1);
            } else if (score.matches("\\d+-")) {
                // X- format => X - 0
                return score.replace("-", " - 0");
            }
        
            return null;
        }

        private boolean validateEmptyString(String value) {
            if (value == null || value.trim().isEmpty()) {
                return false;
            }
            return true;
        }
    }

    public static class DataCleaningReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(null, val);  // Write cleaned data without a specific key
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Cleaning Job");
        job.setJarByClass(DataCleaningJob.class);
        job.setMapperClass(DataCleaningMapper.class);
        job.setReducerClass(DataCleaningReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
