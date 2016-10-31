package VCC_ASS.Parquet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ParseException, IOException
    {
       // System.out.println( "Hello World!" );
        SimpleDateFormat  formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startDate = (Date) formatter.parse("1970-01-01 00:00:00");
        Date birthDay = (Date) formatter.parse("1970-01-01 12:00:00");
  		int  numberDay = (int)TimeUnit.MILLISECONDS.toDays(birthDay.getTime() - startDate.getTime());
  		System.out.println("day = " + numberDay);
  		File file = new File("input.txt");
  		FileWriter fw = new FileWriter(file);
  		BufferedWriter bw = new BufferedWriter(fw);
  		bw.write("dangmc\t" + "1995-12-08 15:15:15\t" + "21\n");
  		bw.write("mliafol\t" + "1995-06-08 08:15:15\t" + "22\n");
  		bw.write("thinh\t" + "1998-03-16 10:40:05\t" + "19\n");
  		bw.close();
  		fw.close();
  		
    }
}