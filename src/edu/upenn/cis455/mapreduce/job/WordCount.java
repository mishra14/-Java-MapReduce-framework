package edu.upenn.cis455.mapreduce.job;

import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job
{
	private HashMap<String, Integer> counts;

	public void map(String key, String value, Context context)
	{
		counts = new HashMap<String, Integer>();
		String[] words = value.split(" ");
		for (String word : words)
		{
			if (counts.containsKey(word))
			{
				int count = counts.get(word);
				counts.put(word, count + 1);
			}
			else
			{
				counts.put(word, 1);
			}
		}
		for (String newKey : counts.keySet())
		{
			context.write(newKey, "" + counts.get(newKey));
		}
	}

	public void reduce(String key, String[] values, Context context)
	{
		// Your reduce function for WordCount goes here
		context.write(key, "" + values.length);
	}

}
