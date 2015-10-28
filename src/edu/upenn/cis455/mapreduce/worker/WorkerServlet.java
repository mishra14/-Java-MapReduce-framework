package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

import javax.servlet.*;
import javax.servlet.http.*;

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

	public void init()
	{
		status = new WorkerStatus(getServletConfig().getInitParameter(
				"selfport"), "NA", "0", "0", WorkerStatus.statusType.idle);
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
}
