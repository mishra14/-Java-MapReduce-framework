package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.upenn.cis455.mapreduce.master.WorkerStatus;

public class PingThread extends Thread
{
	private URL masterUrl;
	private Socket socket;
	private String selfPort;
	private WorkerStatus workerStatus;
	private boolean run;

	public PingThread(URL url, WorkerStatus status)
	{
		this.masterUrl = url;
		this.selfPort = status.getPort();
		this.workerStatus = status;
		this.run = true;
	}

	public void run()
	{
		while (run)
		{
			System.out.println("ping thread : pinging master");
			String host = masterUrl.getHost();
			int port = masterUrl.getPort() == -1 ? masterUrl.getDefaultPort()
					: masterUrl.getPort();
			try
			{
				socket = new Socket(host, port);
				HttpResponse response =  sendPing();
				// System.out.println("ping thread : Response - " + response);
			}
			catch (IOException e)
			{
				System.out
						.println("IOException while opening socket to master");
				e.printStackTrace();
			}
			try
			{
				sleep(10000);
			}
			catch (InterruptedException e)
			{
				System.out.println("Ping Thread interrupted");
				e.printStackTrace();
			}
		}
	}

	/*
	 * http://localhost:8080/master/workerstatus?port=8081&status=idle&keysread=10&keyswritten=5&job=classes
	 */
	public HttpResponse sendPing() throws IOException
	{
		PrintWriter clientSocketOut = new PrintWriter(new OutputStreamWriter(
				socket.getOutputStream()));
		String status;
		String keysRead;
		String keysWritten;
		String job;
		synchronized (workerStatus)
		{
			status = workerStatus.getStatus().toString();
			keysRead = workerStatus.getKeysRead();
			keysWritten = workerStatus.getKeysWritten();
			job = workerStatus.getJob();
		}
		String url = masterUrl + "?port=" + selfPort + "&status=" + status
				+ "&keysread=" + keysRead + "&keyswritten=" + keysWritten
				+ "&job=" + job;
		System.out.println("worker : url - "+url);
		clientSocketOut.print("GET " + url + " HTTP/1.0\r\n");
		clientSocketOut.print("\r\n");
		clientSocketOut.flush();
		return parseResponse();
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

	public void terminate()
	{
		run = false;
	}

	/*public static void main(String[] args)
	{
		WorkerStatus status = new WorkerStatus("8080", "NA", "0", "0",
				WorkerStatus.statusType.idle);
		URL masterUrl;
		PingThread ping;
		try
		{
			masterUrl = new URL("http://" + "127.0.0.1:8080"
					+ "/master/workerStatus");
			ping = new PingThread(masterUrl, status);
			ping.start();
		}
		catch (MalformedURLException e)
		{
			System.out
					.println("URL exception in worker servlet while creating ping thread");
			e.printStackTrace();
		}
	}*/
}
