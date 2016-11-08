package ds.mapreduce;

public class Mapper implements IMapper{
	
	public String map(String inpdata)
	{
		if(inpdata.matches("")) //TODO:put expressions here
			return "<"+inpdata+","+"true>";
		else
			return "";
	}
}
