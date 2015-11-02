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

	/**
	 * The Enum statusType.
	 */
	public static enum statusType
	{

		/** The mapping. */
		mapping,
		/** The waiting. */
		waiting,
		/** The reducing. */
		reducing,
		/** The idle. */
		idle
	};

	/** The port. */
	private String port;

	/** The job. */
	private String job;

	/** The keys read. */
	private String keysRead;

	/** The keys written. */
	private String keysWritten;

	/** The status. */
	private statusType status;

	/** The timestamp. */
	private long timestamp;

	/**
	 * Instantiates a new worker status.
	 *
	 * @param port
	 *            the port
	 * @param job
	 *            the job
	 * @param keysRead
	 *            the keys read
	 * @param keysWritten
	 *            the keys written
	 * @param status
	 *            the status
	 */
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

	/**
	 * Gets the port.
	 *
	 * @return the port
	 */
	public String getPort()
	{
		return port;
	}

	/**
	 * Sets the port.
	 *
	 * @param port
	 *            the new port
	 */
	public void setPort(String port)
	{
		this.port = port;
	}

	/**
	 * Gets the job.
	 *
	 * @return the job
	 */
	public String getJob()
	{
		return job;
	}

	/**
	 * Sets the job.
	 *
	 * @param job
	 *            the new job
	 */
	public void setJob(String job)
	{
		this.job = job;
	}

	/**
	 * Gets the keys read.
	 *
	 * @return the keys read
	 */
	public String getKeysRead()
	{
		return keysRead;
	}

	/**
	 * Sets the keys read.
	 *
	 * @param keysRead
	 *            the new keys read
	 */
	public void setKeysRead(String keysRead)
	{
		this.keysRead = keysRead;
	}

	/**
	 * Gets the keys written.
	 *
	 * @return the keys written
	 */
	public String getKeysWritten()
	{
		return keysWritten;
	}

	/**
	 * Sets the keys written.
	 *
	 * @param keysWritten
	 *            the new keys written
	 */
	public void setKeysWritten(String keysWritten)
	{
		this.keysWritten = keysWritten;
	}

	/**
	 * Gets the status.
	 *
	 * @return the status
	 */
	public statusType getStatus()
	{
		return status;
	}

	/**
	 * Sets the status.
	 *
	 * @param status
	 *            the new status
	 */
	public void setStatus(statusType status)
	{
		this.status = status;
	}

	/**
	 * Gets the timestamp.
	 *
	 * @return the timestamp
	 */
	public long getTimestamp()
	{
		return timestamp;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "WorkerStatus [port=" + port + ", job=" + job + ", keysRead="
				+ keysRead + ", keysWritten=" + keysWritten + ", status="
				+ status + ", timestamp=" + timestamp + "]";
	}

}
