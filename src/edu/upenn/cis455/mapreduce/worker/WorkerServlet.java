package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.master.Job;
import edu.upenn.cis455.mapreduce.master.Queue;
import edu.upenn.cis455.mapreduce.master.WorkerStatus;

public class WorkerServlet extends HttpServlet
{

	static final long serialVersionUID = 455555002;

	public static enum statusType
	{
		mapping, waiting, reducing, idle
	};

	private WorkerStatus status;
	private PingThread pingThread;
	private Job currentJob;
	private File storageDir;
	private Queue<String> lineQueue;
	private boolean doMap;
	private boolean doreduce;
	private List<MapThread> mapThreads;
	private List<ReduceThread> reduceThreads;

	public void init()
	{
		currentJob = null;
		status = new WorkerStatus(getServletConfig().getInitParameter(
				"selfport"), "NA", "0", "0", WorkerStatus.statusType.idle);
		storageDir = new File(getServletConfig().getInitParameter("storagedir"));
		if (!storageDir.exists() || !storageDir.isDirectory())
		{
			storageDir.mkdirs();
		}
		System.out.println("worker servlet : status - " + status);
		URL masterUrl;
		try
		{
			String url = "http://"
					+ getServletConfig().getInitParameter("master")
					+ "/master/workerstatus";
			System.out.println("worker servlet : master url - " + url);
			masterUrl = new URL(url);
			pingThread = new PingThread(masterUrl, status);
			pingThread.start();
		}
		catch (MalformedURLException e)
		{
			System.out
					.println("URL exception in worker servlet while creating ping thread");
			e.printStackTrace();
		}
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException
	{
		System.out.println("worker : post received");
		String pathInfo = request.getPathInfo();
		StringBuilder pageContent = new StringBuilder();
		if (pathInfo.equalsIgnoreCase("/runmap")) // get a map job
		{
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.print("<html>" + pageContent.toString() + "</html>");
			response.flushBuffer();
			System.out.println("worker : /runmap received");
			// read job info from the request and store it
			String job = request.getParameter("job");
			String inputDirectory = request.getParameter("input");
			String numThreads = request.getParameter("numthreads");
			int numWorkers = Integer
					.valueOf(request.getParameter("numworkers") == null ? "0"
							: request.getParameter("numworkers"));
			int threadCount = Integer.valueOf(numThreads);
			currentJob = new Job(job, inputDirectory, null, numThreads, "0");
			ArrayList<String> workers = new ArrayList<String>();
			for (int i = 0; i < numWorkers; i++)
			{
				String worker = request.getParameter("worker" + (i + 1));
				if (worker != null)
				{
					workers.add(worker);
				}
			}
			currentJob.setWorkers(workers);
			System.out.println("worker : new map job - " + currentJob);
			synchronized (status)
			{
				// update status for the ping thread
				status.setJob(currentJob.getJobName());
				status.setKeysRead("0");
				status.setKeysWritten("0");
				status.setStatus(WorkerStatus.statusType.mapping);
			}
			// start map threads
			startMapThreads(threadCount);
			// create a queue of each line of the input
			File inputDir = new File(storageDir.getAbsolutePath()
					+ inputDirectory);
			if (inputDir.exists() && inputDir.isDirectory())
			{
				// read input
				readInput(inputDir);
				// wait for the queue to become empty
				waitforEmptyQueue();
				// now when the queue is empty, turn off all map threads
				setDoMap(false);
				waitForThreads();
				// update status to waiting
				synchronized (status)
				{
					// update status for the ping thread
					status.setStatus(WorkerStatus.statusType.waiting);
					// send ping to master
					if (!pingThread.getState().equals(Thread.State.RUNNABLE))
					{
						pingThread.interrupt();
					}
				}
			}
			// wait for map threads to finish
		}
		else if (pathInfo.equalsIgnoreCase("/runreduce")) // get a reduce job
		{
			System.out.println("worker : /runreduce received");
			synchronized (status)
			{
				status.setJob(request.getParameter("job"));
				status.setKeysRead("0");
				status.setKeysWritten("0");
				status.setStatus(WorkerStatus.statusType.reducing);
			}
		}
	}

	private void waitforEmptyQueue()
	{
		while (lineQueue.getSize() > 0)
		{
			try
			{
				Thread.sleep(1000);
			}
			catch (InterruptedException e)
			{
				System.out
						.println("worker : interrupted while sleeping while checking the queue size");
				e.printStackTrace();
				break;
			}

		}
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException
	{
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}

	public void destroy()
	{
		if (pingThread != null)
		{
			pingThread.terminate();
		}
	}

	public synchronized boolean isDoMap()
	{
		return doMap;
	}

	public synchronized void setDoMap(boolean doMap)
	{
		this.doMap = doMap;
	}

	public synchronized boolean isDoreduce()
	{
		return doreduce;
	}

	public synchronized void setDoreduce(boolean doreduce)
	{
		this.doreduce = doreduce;
	}

	public WorkerStatus getStatus()
	{
		return status;
	}

	public Queue<String> getLineQueue()
	{
		return lineQueue;
	}

	/*public static void main(String[] args) throws IOException
	{
		WorkerServlet servlet = new WorkerServlet();
		String job = "newjob";
		String inputDirectory = "/wordcountinput";
		String numThreads = "1";
		int numWorkers = 1;
		servlet.currentJob = new Job(job, inputDirectory, null, numThreads, "0");
		ArrayList<String> workers = new ArrayList<String>();
		servlet.currentJob.setWorkers(workers);
		System.out.println("worker : new map job - " + servlet.currentJob);
		// start map threads
		servlet.setDoMap(true);
		servlet.mapThreads = new ArrayList<MapThread>();
		for (int i = 0; i < Integer.valueOf(numThreads); i++)
		{
			MapThread mapThread = new MapThread(i, servlet);
			mapThread.start();
			servlet.mapThreads.add(mapThread);
		}
		// create a queue of each line of the input
		servlet.storageDir = new File("/home/cis455/storage");
		File inputDir = new File(servlet.storageDir.getAbsolutePath()
				+ inputDirectory);
		if (inputDir.exists() && inputDir.isDirectory())
		{
			servlet.lineQueue = new Queue<String>();
			for (File file : inputDir.listFiles())
			{
				if (file.isFile() && !file.getName().endsWith("~"))
				{
					FileInputStream fileStream = new FileInputStream(file);
					InputStreamReader in = new InputStreamReader(fileStream);
					BufferedReader buffer = new BufferedReader(in);
					String line = null;
					while ((line = buffer.readLine()) != null)
					{
						servlet.lineQueue.enqueue(line);
					}
					buffer.close();
					in.close();
					fileStream.close();
				}
			}
			System.out.println("worker : input queue - "
					+ servlet.lineQueue.getQueue());
			// wait for the queue to become empty
			while (servlet.lineQueue.getSize() > 0)
			{
				try
				{
					System.out.println("worker : checking queue size");
					Thread.sleep(1000);
				}
				catch (InterruptedException e)
				{
					System.out
							.println("worker : interrupted while sleeping while checking the queue size");
					e.printStackTrace();
					break;
				}

			}
			// now when the queue is empty, turn off all map threads
			servlet.setDoMap(false);
			for (MapThread mapThread : servlet.mapThreads)
			{
				if (mapThread.getState() != Thread.State.RUNNABLE)
				{
					mapThread.interrupt();
				}
			}
			for (MapThread mapThread : servlet.mapThreads)
			{
				try
				{
					mapThread.join();
				}
				catch (InterruptedException e)
				{
					System.out
							.println("worker : interrupted while joining map threads");
					e.printStackTrace();
				}
			}
			System.out.println("done");
		}
		// wait for map threads to finish
	}
	*/
	private void readInput(File inputDir) throws IOException
	{
		lineQueue = new Queue<String>();
		for (File file : inputDir.listFiles())
		{
			if (file.isFile() && !file.getName().endsWith("~"))
			{
				FileInputStream fileStream = new FileInputStream(file);
				InputStreamReader in = new InputStreamReader(fileStream);
				BufferedReader buffer = new BufferedReader(in);
				String line = null;
				while ((line = buffer.readLine()) != null)
				{
					lineQueue.enqueue(line);
				}
				buffer.close();
				in.close();
				fileStream.close();
			}
		}
		System.out.println("worker : input queue - " + lineQueue.getQueue());
	}

	private void startMapThreads(int numThreads)
	{
		setDoMap(true);
		mapThreads = new ArrayList<MapThread>();
		for (int i = 0; i < Integer.valueOf(numThreads); i++)
		{
			MapThread mapThread = new MapThread(i, this);
			mapThread.start();
			mapThreads.add(mapThread);
		}
	}

	private void waitForThreads()
	{
		for (MapThread mapThread : mapThreads)
		{
			if (mapThread.getState() != Thread.State.RUNNABLE)
			{
				mapThread.interrupt();
			}
		}
		for (MapThread mapThread : mapThreads)
		{
			try
			{
				mapThread.join();
			}
			catch (InterruptedException e)
			{
				System.out
						.println("worker : interrupted while joining map threads");
				e.printStackTrace();
			}
		}

	}
}
