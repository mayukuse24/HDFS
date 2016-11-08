package ds.mapreduce;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper implements IMapper{
	
	public String map(String inpdata,String expr)
	{
		Pattern p = Pattern.compile(expr);
	    Matcher m = p.matcher(inpdata);
	    if (m.find())
	    	return inpdata;
		else
			return "";
	}
}
