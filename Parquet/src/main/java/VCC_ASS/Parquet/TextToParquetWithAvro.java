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
 
 
public class TextToParquetWithAvro extends Configured implements Tool {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"name\": \"ParquetFile\",\n" +
      "  \"fields\": [\n" +
      "    {\"name\": \"timeCreate\", \"type\": \"int\"},\n" +
      "    {\"name\": \"cookieCreate\", \"type\": \"int\"},\n" +
      "    {\"name\": \"browserCode\", \"type\": \"int\"},\n"	 +
      "    {\"name\": \"browserVer\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"osCode\", \"type\": \"int\"},\n"	  +
      "    {\"name\": \"osVer\", \"type\": \"string\"},\n"	  +
      "    {\"name\": \"ip\", \"type\": \"long\"},\n"	  +
      "    {\"name\": \"locId\", \"type\": \"int\"},\n"	 +
      "    {\"name\": \"domain\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"siteId\", \"type\": \"int\"},\n"	 +
      "    {\"name\": \"cId\", \"type\": \"int\"},\n"	 +
      "    {\"name\": \"path\", \"type\": \"string\"},\n"	 +
      "    {\"name\": \"referer\", \"type\": \"string\"},\n" +
      "    {\"name\": \"guid\", \"type\": \"long\"},\n"	+
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
      SimpleDateFormat  formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date startDate = null;
      try {
    	  startDate = (Date) formatter.parse("1970-01-01 00:00:00");
      } catch (ParseException e) {
    	  // TODO Auto-generated catch block
    	  e.printStackTrace();
      }
      int cnt = 0;
      while (infor.hasMoreTokens()) {
    	  String val = infor.nextToken();
    	  System.out.println(val);
    	  switch (cnt) {
    	  	case 0:
    	  		try {
    	  			Date timeCreate = (Date) formatter.parse(val);
    	  			int  numberDay = (int)TimeUnit.MILLISECONDS.toDays(timeCreate.getTime() - startDate.getTime());
    	  			record.put(0, numberDay);
    	  		} catch (ParseException e1) {
    	  			// TODO Auto-generated catch block
    	  			e1.printStackTrace();
    	  		}
    	  		
    	  		break;
    	  	case 1:
    	  		Date cookieTime;
    	  		try {
    	  			cookieTime = (Date) formatter.parse(val);
    	  			int  numberDay = (int)TimeUnit.MILLISECONDS.toDays(cookieTime.getTime() - startDate.getTime());
    	  			record.put(1, numberDay);
    	  			break;
    	  		} catch (ParseException e) {
    	  			// TODO Auto-generated catch block
    	  			e.printStackTrace();
    	  		}
    	  		
    	  	case 2:
    	  		record.put(2, Integer.parseInt(val));
    	  		break;
    	  	case 3:
    	  		record.put(3, val);
    	  		break;
    	  	case 4:
    	  		record.put(4, Integer.parseInt(val));
    	  		break;
    	  	case 5:
    	  		record.put(5, val);
    	  		break;
    	  	case 6:
    	  		record.put(6, Long.parseLong(val));
    	  		break;
    	  	case 7: 
    	  		record.put(7, Integer.parseInt(val));
    	  		break;
    	  	case 8: 
    	  		record.put(8, val);
    	  		break;
    	  	case 9:
    	  		record.put(8,  Integer.parseInt(val));
    	  		break;
    	  	case 10:
    	  		record.put(10, Integer.parseInt(val));
    	  		break;
    	  	case 11: 
    	  		record.put(11,  val);
    	  		break;
    	  	case 12: 
    	  		record.put(12,  val);
    	  		break;
    	  	case 13:
    	  		record.put(13, Long.parseLong(val));
    	  		break;
    	  	case 14:
    	  		record.put(14, val);
    	  		break;
    	  	case 15: 
    	  		record.put(15, val);
    	  		break;
    	  	case 16:
    	  		record.put(16, val);
    	  		break;
    	  	case 17:
    	  		record.put(17, val);
    	  		break;
    	  	case 18:
    	  		record.put(18, val);
    	  		break;
    	  }
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
    int exitCode = ToolRunner.run(new TextToParquetWithAvro(), args);
    System.exit(exitCode);
  }
}
