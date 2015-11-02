package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.master.Job;
import edu.upenn.cis455.mapreduce.master.Queue;
import edu.upenn.cis455.mapreduce.master.WorkerStatus;

/**
 * This class is worker servlet class that is responsible for receiving jobs
 * from the master and instantiate the threads for the job
 * 
 * @author cis455
 *
 */
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
	private List<Thread> mapThreads;
	private List<Thread> reduceThreads;
	private Socket socket;

	public void init()
	{
		System.out.println("worker servlet started");
		currentJob = null;
		status = new WorkerStatus(getServletConfig().getInitParameter(
				"selfport"), "NA", "0", "0", WorkerStatus.statusType.idle);
		String storageDirectory = getServletConfig().getInitParameter(
				"storagedir");
		if (storageDirectory.endsWith("/"))
		{
			storageDirectory = storageDirectory.substring(0,
					storageDirectory.length() - 1);
		}
		System.out.println("worker : loading storage directory");
		storageDir = new File(storageDirectory);
		if (!storageDir.exists() || !storageDir.isDirectory())
		{
			storageDir.mkdirs();
		}
		resetSpoolDirectories();
		System.out.println("worker : status - " + status);
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
			lineQueue = new Queue<String>();
			startMapThreads(threadCount);
			// create a queue of each line of the input
			File inputDir = new File(storageDir.getAbsolutePath()
					+ inputDirectory);
			if (inputDir.exists() && inputDir.isDirectory())
			{
				// read input
				try
				{
					readInput(inputDir, true);
				}
				catch (InterruptedException e)
				{
					System.out
							.println("worker : exception while reading reduce input file");
					e.printStackTrace();
				}
				// wait for the queue to become empty
				waitforEmptyQueue();
				// now when the queue is empty, turn off all map threads
				setDoMap(false);
				waitForThreads(true);
				File spoolOutDir = new File(storageDir.getAbsolutePath()
						+ "/spoolout");
				sendSpoolOut(spoolOutDir);
				// update status to waiting
				synchronized (status)
				{
					// update status for the ping thread
					status.setStatus(WorkerStatus.statusType.waiting);
				}
				// send ping to master
				if (!pingThread.getState().equals(Thread.State.RUNNABLE))
				{
					pingThread.interrupt();
				}
			}
			// wait for map threads to finish
		}
		else if (pathInfo.equalsIgnoreCase("/runreduce")) // get a reduce job
		{
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.print("<html>" + pageContent.toString() + "</html>");
			response.flushBuffer();

			System.out.println("worker : /runreduce received");
			// read job info from the request and store it
			String outputDirectory = request.getParameter("output");
			String numThreads = request.getParameter("numthreads");
			int threadCount = Integer.valueOf(numThreads);
			currentJob.setReduceThreads(numThreads);
			currentJob.setOutputDirectory(outputDirectory);
			System.out.println("worker : new reduce job - " + currentJob);
			synchronized (status)
			{
				// update status for the ping thread
				status.setStatus(WorkerStatus.statusType.reducing);
				status.setKeysRead("0");
				status.setKeysWritten("0");
			}
			// start map threads
			lineQueue = new Queue<String>();
			resetOutputDirectory();
			startReduceThreads(threadCount);
			// create a queue of each line of the input
			File inputDir = new File(storageDir.getAbsolutePath() + "/spoolin");
			if (inputDir.exists() && inputDir.isDirectory())
			{
				// read input
				try
				{
					readInput(inputDir, false);
				}
				catch (InterruptedException e)
				{
					System.out
							.println("worker : exception while reading reduce input file");
					e.printStackTrace();
				}
				// wait for the queue to become empty
				waitforEmptyQueue();
				// now when the queue is empty, turn off all map threads
				setDoreduce(false);
				waitForThreads(false);
				resetSpoolDirectories();

				// update status to waiting
				synchronized (status)
				{
					// update status for the ping thread
					status.setStatus(WorkerStatus.statusType.idle);
					status.setKeysRead("0");
				}
				// send ping to master
				pingThread.interrupt();
			}
		}
		else if (pathInfo.equalsIgnoreCase("/pushdata")) // get a reduce job
		{
			System.out.println("worker : /pushdata received");
			writeToSpoolIn(request);
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.print("<html>" + pageContent.toString() + "</html>");
			response.flushBuffer();
		}
	}

	private void writeToSpoolIn(HttpServletRequest request) throws IOException
	{
		File spoolInFile = new File(storageDir.getAbsolutePath() + "/spoolin/"
				+ request.getRemoteAddr() + ":" + request.getRemotePort()
				+ ".txt");
		if (spoolInFile.exists())
		{
			spoolInFile.delete();
		}
		spoolInFile.createNewFile();

		FileWriter fileWriter = new FileWriter(spoolInFile);
		BufferedReader reader = request.getReader();
		String line;
		while ((line = reader.readLine()) != null)
		{
			fileWriter.append(line + "\r\n");
		}
		fileWriter.close();
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

	public Job getCurrentJob()
	{
		return currentJob;
	}

	public File getStorageDir()
	{
		return storageDir;
	}

	private void readInput(File inputDir, boolean isMap) throws IOException,
			InterruptedException
	{
		BufferedReader reader = null;
		if (!isMap)
		{
			System.out.println("worker : creating script");
			File script = new File(inputDir.getAbsolutePath() + "/runsort.sh");
			if (!script.exists())
			{
				script.createNewFile();
				FileWriter writer = new FileWriter(script);
				writer.append("sort -k 1 -t \\t " + inputDir.getAbsolutePath()
						+ "/*.txt | sort -m");
				writer.close();
				script.setExecutable(true);
			}
			Runtime runtime = Runtime.getRuntime();
			Process p = runtime.exec(script.getAbsolutePath());// pb.start();
			int result = p.waitFor();
			if (result == 0)
			{
				System.out.println("worker : sort done correctly");
				reader = new BufferedReader(new InputStreamReader(
						p.getInputStream()));
				String line = null;
				String lastLine = null;
				StringBuffer lines = new StringBuffer();
				int n = 0;
				while ((line = reader.readLine()) != null)
				{
					n++;
					/*System.out.println("line - " + line);
					System.out.println("last line - " + lastLine);*/
					// System.out.println("lines - "+lines.toString());
					if (lastLine == null
							|| (lastLine.split("\t")[0]
									.equals(line.split("\t")[0])))
					{
						lines.append(line + "\r\n");
					}
					else
					{
						// System.out.println("writing to queue - "+
						// lines.toString());
						lineQueue.enqueue(new String(lines.toString()));
						lines.setLength(0);
						lines.append(line + "\r\n");
					}
					lastLine = line;
				}
				System.out.println("Lines read - " + n);
				lineQueue.enqueue(new String(lines.toString()));
				reader.close();
			}
			else
			{
				reader = new BufferedReader(new InputStreamReader(
						p.getErrorStream()));
				System.out.println("worker : error while sorting file - ");
				String line;
				while ((line = reader.readLine()) != null)
				{
					System.out.println(line);
				}
			}
		}
		else
		{
			for (File file : inputDir.listFiles())
			{
				if (file.isFile() && !file.getName().endsWith("~"))
				{
					FileInputStream fileStream = new FileInputStream(file);
					InputStreamReader in = new InputStreamReader(fileStream);
					BufferedReader buffer = isMap ? new BufferedReader(in)
							: reader;
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
		}

		System.out.println("worker : input queue - " + lineQueue.getQueue());
	}

	private void startMapThreads(int numThreads)
	{
		setDoMap(true);
		mapThreads = new ArrayList<Thread>();
		for (int i = 0; i < Integer.valueOf(numThreads); i++)
		{
			MapThread mapThread = new MapThread(i, this);
			mapThread.start();
			mapThreads.add(mapThread);
		}
	}

	private void startReduceThreads(int numThreads)
	{
		setDoreduce(true);
		reduceThreads = new ArrayList<Thread>();
		for (int i = 0; i < Integer.valueOf(numThreads); i++)
		{
			ReduceThread reduceThread = new ReduceThread(i, this);
			reduceThread.start();
			reduceThreads.add(reduceThread);
		}
	}

	private void waitForThreads(boolean isMap)
	{
		List<Thread> threads;
		if (isMap)
		{
			threads = mapThreads;
		}
		else
		{
			threads = reduceThreads;
		}
		for (Thread thread : threads)
		{
			if (thread.getState() != Thread.State.RUNNABLE)
			{
				thread.interrupt();
			}
		}
		for (Thread thread : threads)
		{
			try
			{
				thread.join();
			}
			catch (InterruptedException e)
			{
				System.out
						.println("worker : interrupted while joining map threads");
				e.printStackTrace();
			}
		}
	}

	public void resetSpoolDirectories()
	{
		System.out.println("worker : reseting spool directories");
		File spoolOutDir = new File(storageDir.getAbsolutePath() + "/spoolout");
		if (spoolOutDir.exists())
		{
			if (spoolOutDir.isDirectory())
			{
				for (File file : spoolOutDir.listFiles())
				{
					file.delete();
				}
			}
			spoolOutDir.delete();
		}
		if (!spoolOutDir.exists() || !spoolOutDir.isDirectory())
		{
			spoolOutDir.mkdirs();
		}

		File spoolInDir = new File(storageDir.getAbsolutePath() + "/spoolin");
		if (spoolInDir.exists())
		{
			if (spoolInDir.isDirectory())
			{
				for (File file : spoolInDir.listFiles())
				{
					file.delete();
				}
			}
			spoolInDir.delete();
		}
		if (!spoolInDir.exists() || !spoolInDir.isDirectory())
		{
			spoolInDir.mkdirs();
		}
	}

	public void resetOutputDirectory()
	{
		System.out.println("worker : reseting output directory");
		File outputDir = new File(getStorageDir().getAbsolutePath()
				+ getCurrentJob().getOutputDirectory());
		if (outputDir.exists())
		{
			if (outputDir.isDirectory())
			{
				for (File file : outputDir.listFiles())
				{
					file.delete();
				}
			}
		}
	}

	private void sendSpoolOut(File spoolOutDir) throws IOException
	{
		for (File file : spoolOutDir.listFiles())
		{
			if (file.isFile() && !file.getName().endsWith("~"))
			{
				String workerUrl = "http://" + file.getName()
						+ "/worker/pushdata";

				String body = readFile(file);
				URL url = new URL(workerUrl);
				String host = url.getHost();
				int port = url.getPort() == -1 ? url.getDefaultPort() : url
						.getPort();
				socket = new Socket(host, port);
				PrintWriter clientSocketOut = new PrintWriter(
						new OutputStreamWriter(socket.getOutputStream()));
				clientSocketOut.print("POST " + url.toString()
						+ " HTTP/1.0\r\n");
				clientSocketOut.print("Content-Length:" + body.length()
						+ "\r\n");
				clientSocketOut.print("Content-Type:text/plain\r\n");
				clientSocketOut.print("\r\n");
				clientSocketOut.print(body);
				clientSocketOut.print("\r\n");
				clientSocketOut.print("\r\n");
				clientSocketOut.flush();
				HttpResponse response = parseResponse();
				if (!response.getResponseCode().equalsIgnoreCase("200"))
				{
					System.out.println("Master : worker " + file.getName()
							+ "did not accept the pushdata");
				}
			}
		}
	}

	public HttpResponse parseResponse() throws IOException
	{
		InputStream socketInputStream = socket.getInputStream();
		InputStreamReader socketInputStreamReader = new InputStreamReader(
				socketInputStream);
		BufferedReader socketBufferedReader = new BufferedReader(
				socketInputStreamReader);
		HttpResponse response = parseResponse(socketBufferedReader);
		socketBufferedReader.close();
		socketInputStreamReader.close();
		socketInputStream.close();
		socket.close();
		return response;
	}

	/**
	 * parses the http response from the server into an HttpResponse object
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public HttpResponse parseResponse(BufferedReader in) throws IOException
	{
		HttpResponse response = new HttpResponse();
		String line = in.readLine();
		if (line != null)
		{
			String[] firstLineSplit = line.trim().split(" ", 3);
			if (firstLineSplit.length < 3)
			{
				return null;
			}
			if (firstLineSplit[0].trim().split("/").length < 2)
			{
				return null;
			}
			response.setProtocol((firstLineSplit[0].trim().split("/")[0]));
			response.setVersion((firstLineSplit[0].trim().split("/")[1]));
			response.setResponseCode(firstLineSplit[1].trim());
			response.setResponseCodeString(firstLineSplit[2].trim());
			Map<String, List<String>> headers = new HashMap<String, List<String>>();
			while ((line = in.readLine()) != null)
			{
				if (line.equals(""))
				{
					break;
				}
				String[] lineSplit = line.trim().split(":", 2);
				if (lineSplit.length == 2)
				{
					if (headers.containsKey(lineSplit[0].toLowerCase().trim()))
					{
						headers.get(lineSplit[0]).add(lineSplit[1].trim());
					}
					else
					{
						ArrayList<String> values = new ArrayList<String>();
						values.add(lineSplit[1].trim());
						headers.put(lineSplit[0].toLowerCase().trim(), values);
					}

				}
			}
			StringBuilder responseBody = new StringBuilder();
			while ((line = in.readLine()) != null)
			{
				responseBody.append(line + "\r\n");
			}
			response.setHeaders(headers);
			response.setData(responseBody.toString());
		}
		else
		{
			return null;
		}
		return response;
	}

	private String readFile(File file) throws IOException
	{
		FileInputStream fileInputStream = new FileInputStream(file);
		byte[] fileBody = new byte[(int) file.length()];
		fileInputStream.read(fileBody);
		fileInputStream.close();

		String body = new String(fileBody, "UTF-8");

		return body;
	}

}
