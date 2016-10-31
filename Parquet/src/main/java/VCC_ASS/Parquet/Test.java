package VCC_ASS.Parquet;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

public class Test {
	
	public static void main(String []args) throws FileNotFoundException {
		Scanner sc = new Scanner(new File("input.txt"));
		while (sc.hasNextLine()) {
			String line = sc.nextLine();
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
		    	  switch (cnt) {
		    	  	case 0:
		    	  		System.out.println(val);
		    	  		break;
		    	  	case 1:
					Date birthDay;
					try {
						birthDay = (Date) formatter.parse(val);
						
						int  numberDay = (int)TimeUnit.MILLISECONDS.toDays(birthDay.getTime() - startDate.getTime());
			
		    	  		System.out.println(numberDay);
		    	  		break;
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    	  		

		    	  	default:
		    	  		System.out.println(Integer.parseInt(val));
		    	  		break;
		    	  }
		    	  cnt++;
		      }
		}
		sc.close();
	}
}
