package edu.upenn.cis455.mapreduce.master;

import java.util.Date;

/**
 * This is a bean class to hold all the information about a worker. This class
 * is used by the master.
 * 
 * @author cis455
 *
 */
public class WorkerStatus
{
	public static enum statusType
	{
		mapping, waiting, reducing, idle
	};

	private String port;
	private String job;
	private String keysRead;
	private String keysWritten;
	private statusType status;
	private long timestamp;

	public WorkerStatus(String port, String job, String keysRead,
			String keysWritten, statusType status)
	{
		super();
		this.port = port;
		this.job = job;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
		this.status = status;
		this.timestamp = (new Date()).getTime();
	}

	public String getPort()
	{
		return port;
	}

	public void setPort(String port)
	{
		this.port = port;
	}

	public String getJob()
	{
		return job;
	}

	public void setJob(String job)
	{
		this.job = job;
	}

	public String getKeysRead()
	{
		return keysRead;
	}

	public void setKeysRead(String keysRead)
	{
		this.keysRead = keysRead;
	}

	public String getKeysWritten()
	{
		return keysWritten;
	}

	public void setKeysWritten(String keysWritten)
	{
		this.keysWritten = keysWritten;
	}

	public statusType getStatus()
	{
		return status;
	}

	public void setStatus(statusType status)
	{
		this.status = status;
	}

	public long getTimestamp()
	{
		return timestamp;
	}

	@Override
	public String toString()
	{
		return "WorkerStatus [port=" + port + ", job=" + job + ", keysRead="
				+ keysRead + ", keysWritten=" + keysWritten + ", status="
				+ status + ", timestamp=" + timestamp + "]";
	}

}
