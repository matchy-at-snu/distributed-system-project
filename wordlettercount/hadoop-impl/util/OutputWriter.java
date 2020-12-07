package DSProject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class OutputWriter {
    public static void writeToOutput(String toWrite) {

    }

    public static void writeToOutput(String src, String dest) throws IOException {
        StringBuilder prevResult = new StringBuilder(src).append("/part-r-00000");
        try (FileReader in = new FileReader(prevResult.toString()); FileWriter out = new FileWriter(dest)) {
            ArrayList<String> lines = new ArrayList<>();

            String header = String.format("|%10s|%15s|%15s|%15s|\n",
                                          "Rank", "Word", "Category", "Frequency");

            BufferedReader bf = new BufferedReader(in, 10 * 1024);

            int lineCount = 0;
            String line = bf.readLine();
            while (line != null) {
                lines.add(line);
                lineCount++;
                line = bf.readLine();
            }

            assert lineCount > 0;

            int size5Percent = (int) Math.ceil(0.05 * lineCount);
            int CommonStart = (lineCount - size5Percent)/2;
            int CommonEnd = (lineCount + size5Percent)/2;
            int Rare = lineCount - size5Percent - 1;

            // Write Popular
            out.write(header);
            int rank = 1;
            for (String l : lines.subList(0, size5Percent)) {
                int splitPoint = l.indexOf("\t");
                String letter = l.substring(0, splitPoint);
                String count = l.substring(splitPoint + 1);
                String fString = String.format("|%10d|%15s|%15s|%15s|\n",
                                               rank, letter, "Popular", count);
                out.write(fString);
                rank++;
            }
            out.write("\n");
            out.write(header);
            rank = CommonStart + 1;
            for (String l : lines.subList(CommonStart, CommonEnd)) {
                int splitPoint = l.indexOf("\t");
                String letter = l.substring(0, splitPoint);
                String count = l.substring(splitPoint + 1);
                String fString = String.format("|%10d|%15s|%15s|%15s|\n",
                                               rank, letter, "Common", count);
                out.write(fString);
                rank++;
            }

            out.write("\n");
            out.write(header);
            rank = Rare + 1;
            for (String l : lines.subList(Rare, lineCount - 1)) {
                int splitPoint = l.indexOf("\t");
                String letter = l.substring(0, splitPoint);
                String count = l.substring(splitPoint + 1);
                String fString = String.format("|%10d|%15s|%15s|%15s|\n",
                                               rank, letter, "Rare", count);
                out.write(fString);
                rank++;
            }
        }
    }
}
