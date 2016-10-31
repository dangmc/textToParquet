package VCC_ASS.Parquet;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetOutputFormat;
import parquet.example.data.Group;
 
 
public class TextToParquetWithAvroString extends Configured implements Tool {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"name\": \"ParquetFile\",\n" +
      "  \"fields\": [\n" +
      "    {\"name\": \"timeCreate\", \"type\": \"string\"},\n" +
      "    {\"name\": \"cookieCreate\", \"type\": \"string\"},\n" +
      "    {\"name\": \"browserCode\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"browserVer\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"osCode\", \"type\": \"string\"},\n"	  +
      "    {\"name\": \"osVer\", \"type\": \"string\"},\n"	  +
      "    {\"name\": \"ip\", \"type\": \"string\"},\n"	  +
      "    {\"name\": \"locId\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"domain\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"siteId\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"cId\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"path\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"referer\", \"type\": \"string\"},\n" +
      "    {\"name\": \"guid\", \"type\": \"string\"},\n"	+
      "    {\"name\": \"flashVersion\", \"type\": \"string\"},\n" +
      "    {\"name\": \"jre\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"sr\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"sc\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"geographic\", \"type\": \"string\"}\n"	 +
      "  ]\n" +
      "}");

  public static class TextToParquetMapper
      extends Mapper<LongWritable, Text, Void, GenericRecord> {

    private GenericRecord record = new GenericData.Record(SCHEMA);

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer infor = new StringTokenizer(line, "\t");
      
      int cnt = 0;
      while (infor.hasMoreTokens()) {
    	  String val = infor.nextToken();
    	  record.put(cnt, val);
    	  cnt++;
    	  if (cnt > 18) break;
      }
      context.write(null, record);
    }
  }


  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    Job job = new Job(getConf(), "Text to Parquet");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(TextToParquetMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    AvroParquetOutputFormat.setSchema(job, SCHEMA);

    job.setOutputKeyClass(Void.class);
    job.setOutputValueClass(Group.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TextToParquetWithAvroString(), args);
    System.exit(exitCode);
  }
}
