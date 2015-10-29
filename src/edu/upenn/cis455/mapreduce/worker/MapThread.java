package edu.upenn.cis455.mapreduce.worker;

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
					System.out
							.println("Map thread " + id + " : line - " + line);
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
