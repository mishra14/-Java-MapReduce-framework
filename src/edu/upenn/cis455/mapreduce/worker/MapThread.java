package edu.upenn.cis455.mapreduce.worker;

import java.io.File;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.WordCount;
import edu.upenn.cis455.mapreduce.job.WordCountContext;

public class MapThread extends Thread
{
	private int id;
	private WorkerServlet workerServlet;

	public MapThread(int id, WorkerServlet workerServlet)
	{
		super();
		this.id = id;
		this.workerServlet = workerServlet;
	}

	public void run()
	{
		System.out.println("worker map thread " + id + " : started");
		while (workerServlet.isDoMap())
		{
			synchronized (workerServlet.getLineQueue())
			{
				if (workerServlet.getLineQueue().getSize() > 0)
				{
					String line = workerServlet.getLineQueue().dequeue();
					String key = line.split("\t")[0];
					String value = line.split("\t")[1];
					System.out.println("Map thread " + id + " : key - " + key
							+ ", value - " + value);
					try
					{
						String className = workerServlet.getCurrentJob()
								.getJobName();
						Class<?> jobClass = Class.forName(className);
						Job job = new WordCount();
						File spoolOutDir = new File(workerServlet
								.getStorageDir().getAbsolutePath()
								+ "/spoolout");
						WordCountContext context = new WordCountContext(
								workerServlet.getCurrentJob().getWorkers(),
								spoolOutDir.getAbsolutePath(), null, true);
						job.map(key, value, context);
					}
					catch (ClassNotFoundException e)
					{
						System.out
								.println("worker map thread : Exception while instantiating job class");
						e.printStackTrace();
					}
					/*					catch (InstantiationException e)
										{
											System.out
													.println("worker map thread : Exception while instantiating job class");
											e.printStackTrace();
										}
										catch (IllegalAccessException e)
										{
											System.out
													.println("worker map thread : Exception while instantiating job class");
											e.printStackTrace();
										}*/

				}
				else
				{
					try
					{
						System.out.println("worker map thread " + id
								+ " : waiting on line queue");
						workerServlet.getLineQueue().wait();
						System.out.println("worker map thread " + id
								+ " : done waiting for line queue");
					}
					catch (InterruptedException e)
					{
						if (workerServlet.isDoMap())
						{
							System.out
									.println("Map thread "
											+ id
											+ " : Exception while waiting for line queue");
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
}
