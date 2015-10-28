package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet
{

	static final long serialVersionUID = 455555001;
	private static final String css = "<head>" + "<style>" + "table, th, td {"
			+ "    border: 1px solid black;" + "    border-collapse: collapse;"
			+ "}" + "th, td {" + "    padding: 5px;" + "}" + "</style>"
			+ "</head>";
	private Map<String, WorkerStatus> workers;
	private Queue<Job> jobQueue;

	public void init()
	{
		workers = new HashMap<String, WorkerStatus>();
		jobQueue = new Queue<Job>();
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		String pathInfo = request.getPathInfo();
		StringBuilder pageContent = new StringBuilder();
		if (pathInfo == null || pathInfo.equalsIgnoreCase("/")) // homepage
		{
			response.sendRedirect("/");
		}
		else if (pathInfo.equalsIgnoreCase("/newjob"))
		{
			// create new job
			String jobName = request.getParameter("job");
			String inputDirectory = request.getParameter("inputdir");
			String outputDirectory = request.getParameter("outputdir");
			String mapThreads = request.getParameter("mapthreads");
			String reduceThreads = request.getParameter("reducethreads");
			pageContent.append("creating new job with following params - <br>"
					+ "Job : " + request.getParameter("job") + "<br>"
					+ "Input Directory : " + request.getParameter("inputdir")
					+ "<br>" + "Output Directory : "
					+ request.getParameter("outputdir") + "<br>"
					+ "Map Threads : " + request.getParameter("mapthreads")
					+ "<br>" + "Reduce Threads : "
					+ request.getParameter("reducethreads") + "<br>");
			Job newJob = new Job(jobName, inputDirectory, outputDirectory,
					mapThreads, reduceThreads);
			jobQueue.enqueue(newJob);
			// TODO - start a job at this point and go back to /status
			if (jobQueue.getSize() == 1)
			{
				// start this job as this is the only job
				assignJob();
			}

		}
		PrintWriter out = response.getWriter();
		out.print("<html>" + css + pageContent.toString() + "</html>");
		response.flushBuffer();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException
	{
		response.setContentType("text/html");

		String pathInfo = request.getPathInfo();
		StringBuilder pageContent = new StringBuilder();
		if (pathInfo == null || pathInfo.equalsIgnoreCase("/")) // homepage
		{
			pageContent.append(getHomePage());
		}
		else if (pathInfo.equalsIgnoreCase("/status"))
		{
			pageContent.append(getStatusPage());
		}
		else if (pathInfo.equalsIgnoreCase("/workerstatus"))
		{
			String port = request.getParameter("port");
			WorkerStatus.statusType type = WorkerStatus.statusType
					.valueOf(request.getParameter("status"));
			String keysRead = request.getParameter("keysread");
			String keysWritten = request.getParameter("keyswritten");
			String job = request.getParameter("job");
			WorkerStatus workerStatus = new WorkerStatus(port, job, keysRead,
					keysWritten, type);
			workers.put(request.getRemoteAddr() + ":" + port, workerStatus);

			if (type == WorkerStatus.statusType.idle)
			{
				// check if any job is running
				Job firstJob = (jobQueue.getSize()>0)?jobQueue.getQueue().get(0):null;
				if (firstJob != null && firstJob.getWorkers() == null)
				{
					// if not then assign a new job
					assignJob();
				}
				else if (firstJob != null && firstJob.getWorkers() != null)
				{
					boolean stillWorking = false;
					// a job is running; check if this is done
					for (String workerName : firstJob.getWorkers())
					{
						WorkerStatus worker = workers.get(workerName);
						if (worker != null
								&& worker.getStatus() != WorkerStatus.statusType.idle)
						{
							stillWorking = true;
							break;
						}
					}
					if (!stillWorking)
					{
						// this job is done
						// remove from queue
						jobQueue.dequeue();
						assignJob();
					}
				}
				// else do nothing
			}
			else if (type == WorkerStatus.statusType.waiting)
			{
				// check if all current job workers have moved to waiting
			}
			// check if the current job is done
			// if done then assign the next job, if any
			// else do nothing
			pageContent.append("Updated worker - " + workerStatus.toString());
		}
		else
		{
			pageContent.append("<head><title>Master</title></head>"
					+ "<body>Unknown Url<br><br>"
					+ "Please use <a href=\"/master/status\">status page</a> "
					+ " to give a new job and check worker status !<br>"
					+ "</body>");
		}
		PrintWriter out = response.getWriter();
		out.print("<html>" + css + pageContent.toString() + "</html>");
		response.flushBuffer();
	}

	private String getHomePage()
	{
		return "<head><title>Master</title></head>"
				+ "<body><h2>Master Servlet home page </h2><br><br>"
				+ "Please use <a href=\"/master/status\">status page</a> "
				+ " to give a new job and check worker status !<br>"
				+ "</body>";
	}

	private String getStatusPage()
	{
		StringBuilder pageContent = new StringBuilder();

		pageContent.append("<head><title>Master</title></head>"
				+ "<body><h2>Master Servlet status page </h2><br><br>");
		pageContent.append("Submit new map reduce job - <br><br>"
				+ "<form action=\"/master/newjob\" method=\"post\">"
				+ "Job class name:<br>"
				+ "<input type=\"text\" name =\"job\"><br>"
				+ "Input directory(relative to worker storage directory):<br>"
				+ "<input type=\"text\" name =\"inputdir\"><br>"
				+ "Output directory(relative to worker storage directory):<br>"
				+ "<input type=\"text\" name =\"outputdir\"><br>"
				+ "Number of map threads:<br>"
				+ "<input type=\"text\" name =\"mapthreads\"><br>"
				+ "Number of reduce threads:<br>"
				+ "<input type=\"text\" name =\"reducethreads\"><br>"
				+ "<input type=\"submit\" value =\"Submit\">"
				+ "</form><br><br>");

		pageContent.append("Worker status - <br>" + "<table>"
				+ "<th>IP:Port</th>" + "<th>Status</th>" + "<th>Job</th>"
				+ "<th>Keys Read</th>" + "<th>Keys Written</th>");
		for (Map.Entry<String, WorkerStatus> entry : workers.entrySet())
		{
			long now = (new Date()).getTime();
			if (now - entry.getValue().getTimestamp() <= 30000)
			{
				pageContent
						.append("<tr>" + "<td>" + entry.getKey() + "</td>"
								+ "<td>" + entry.getValue().getStatus()
								+ "</td>" + "<td>" + entry.getValue().getJob()
								+ "</td>" + "<td>"
								+ entry.getValue().getKeysRead() + "</td>"
								+ "<td>" + entry.getValue().getKeysWritten()
								+ "</td>" + "</tr>");
			}

		}

		pageContent.append("</table><br><br>Job Queue - <br>" + "<table>"
				+ "<th>Job Name</th>" + "<th>Input Directory</th>"
				+ "<th>Output Directory</th>"
				+ "<th>Number of Map Threads</th>"
				+ "<th>Number of Reduce Threads</th>" + "<th>Status</th>");
		for (Job job : jobQueue.getQueue())
		{
			pageContent.append("<tr>" + "<td>" + job.getJobName() + "</td>"
					+ "<td>" + job.getInputDirectory() + "</td>" + "<td>"
					+ job.getOutputDirectory() + "</td>" + "<td>"
					+ job.getMapThreads() + "</td>" + "<td>"
					+ job.getReduceThreads() + "</td>" + "<td>"
					+ ((job.getWorkers() == null) ? "Pending" : "In Process")
					+ "</td>" + "</tr>");
		}
		pageContent.append("</table></body>");

		return pageContent.toString();
	}

	private void assignJob()
	{
		if (jobQueue.getSize() > 0)
		{
			Job newJob = jobQueue.getQueue().get(0);
			if (newJob != null)
			{
				ArrayList<String> jobWorkers = new ArrayList<String>();
				for (Map.Entry<String, WorkerStatus> entry : workers.entrySet())
				{
					long now = (new Date()).getTime();
					if (now - entry.getValue().getTimestamp() <= 30000)
					{
						// send new post /runmap request to each active worker
						// TODO send the requests
						jobWorkers.add(entry.getKey());
					}
				}
				if (jobWorkers.size() > 0)
				{
					// job was assigned to atleast 1 worker
					newJob.setWorkers(jobWorkers);
				}
			}
		}
	}
}
