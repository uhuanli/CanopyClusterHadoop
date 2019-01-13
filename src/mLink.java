import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

public class mLink
{
	public int Movie;
	HashMap<Integer, Integer> UR = new HashMap<Integer, Integer>();
	HashSet<Integer> CNP = new HashSet<Integer>();
	
	public void setLink(int _movie, String _val)
	{
		Scanner sc = new Scanner(_val);
		Movie = _movie;
		int _uid, _rating;
		int _len = sc.nextInt();
		UR.clear();
		for(int i = 0; i < _len; i ++)
		{
			_uid = sc.nextInt();
			_rating = sc.nextInt();
			if(i >= 10000) continue;
			UR.put(_uid, _rating);
		}
		int cp_len = sc.nextInt();
		for(int i = 0; i < cp_len; i ++)
		{
			CNP.add(sc.nextInt());
		}
	}
	
	public boolean commonCanopy(mLink _m)
	{
		for(int _uid : _m.CNP)
		{
			if(CNP.contains(_uid))	return true;
		}
		return false;
	}
	
	public int calDist(mLink _m)
	{
		int _dist = 0;
		for(int _uid : _m.UR.keySet())
		{
			if(UR.keySet().contains(_uid))
			{
				_dist ++;
			}
		}
		return _dist;
	}
	
	public String formVal()
	{
		int ur_len = UR.size();
		StringBuffer ival = new StringBuffer(ur_len * 4);
		ival.append(ur_len + "");
		for(int _uid : UR.keySet())
		{
			ival.append(" ").append(_uid).append(" ").append(UR.get(_uid));
		}
		int cp_len = CNP.size();
		ival.append(" ").append(cp_len);
		for(int _uid : CNP)
		{
			ival.append(" ").append(_uid);
		}
		String _s = ival.toString();
		return _s;
	}
	
	public void addUR(int _uid, int _rating)
	{
		UR.put(_uid, _rating);
	}
	
	public void addCNP(int _cnp)
	{
		CNP.add(_cnp);
	}
	
	public int nUR()
	{
		return UR.size();
	}
}