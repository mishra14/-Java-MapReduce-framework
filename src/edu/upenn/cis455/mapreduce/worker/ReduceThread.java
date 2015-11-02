package edu.upenn.cis455.mapreduce.worker;

import java.io.File;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.WordCountContext;

/**
 * This is a reduce thread that is instantiated by the worker servlet to perform
 * the reduce job on key value inputs
 * 
 * @author cis455
 *
 */

public class ReduceThread extends Thread
{
	private int id;
	private WorkerServlet workerServlet;

	public ReduceThread(int id, WorkerServlet workerServlet)
	{
		super();
		this.id = id;
		this.workerServlet = workerServlet;
	}

	public void run()
	{
		System.out.println("worker reduce thread " + id + " : started");
		while (workerServlet.isDoreduce())
		{
			synchronized (workerServlet.getLineQueue())
			{
				if (workerServlet.getLineQueue().getSize() > 0)
				{
					String line = workerServlet.getLineQueue().dequeue();
					synchronized (workerServlet.getStatus())
					{
						// update status for the ping thread
						int count = Integer.valueOf(workerServlet.getStatus()
								.getKeysRead());
						count++;
						workerServlet.getStatus().setKeysRead("" + (count));
					}
					System.out.println("reduce thread : read line - " + line);
					String[] lines = line.split("\r\n");
					String key = lines[0].split("\t")[0];
					String[] values = new String[lines.length];
					for (int i = 0; i < lines.length; i++)
					{
						values[i] = lines[i].split("\t")[1];
					}
					String className = workerServlet.getCurrentJob()
							.getJobName();
					try
					{
						// TODO generalize class and context
						Class<?> jobClass = Class.forName(className);
						Job job = (Job) jobClass.newInstance();
						File outputDir = new File(workerServlet.getStorageDir()
								.getAbsolutePath()
								+ workerServlet.getCurrentJob()
										.getOutputDirectory());
						WordCountContext context = new WordCountContext(null,
								null, outputDir.getAbsolutePath(), false, null);
						job.reduce(key, values, context);
						synchronized (workerServlet.getStatus())
						{
							// update status for the ping thread
							int count = Integer.valueOf(workerServlet
									.getStatus().getKeysWritten());
							count++;
							workerServlet.getStatus().setKeysWritten(
									"" + (count));
						}
						System.out.println("reduce thread " + id + " : line - "
								+ line);
					}
					catch (ClassNotFoundException e)
					{
						System.out
								.println("worker reduce thread : Exception while instantiating job class");
						e.printStackTrace();
					}
					catch (InstantiationException e)
					{
						System.out
								.println("worker reduce thread : Exception while instantiating job class");
						e.printStackTrace();
					}
					catch (IllegalAccessException e)
					{
						System.out
								.println("worker reduce thread : Exception while instantiating job class");
						e.printStackTrace();
					}

				}
				else
				{
					try
					{
						workerServlet.getLineQueue().wait();
					}
					catch (InterruptedException e)
					{
						if (workerServlet.isDoreduce())
						{
							System.out
									.println("reduce thread "
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
