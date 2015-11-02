package edu.upenn.cis455.mapreduce.job;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.master.WorkerStatus;

/**
 * This class is an implementation of context interface for the wordcount
 * problem.
 *
 * @author cis455
 */
public class WordCountContext implements Context
{

	/** The max. */
	private static BigInteger max = new BigInteger(
			"1461501637330902918203684832716283019655932542975");

	/** The workers. */
	private List<String> workers;

	/** The worker count. */
	private BigInteger workerCount;

	/** The spool out dir. */
	private String spoolOutDir;

	/** The output dir. */
	private String outputDir;

	/** The is map. */
	private boolean isMap;

	/** The status. */
	private WorkerStatus status;

	/**
	 * constructor to pass the needed information to the context.
	 *
	 * @param workers
	 *            the workers
	 * @param spoolOutDir
	 *            the spool out dir
	 * @param outputDir
	 *            the output dir
	 * @param isMap
	 *            the is map
	 * @param status
	 *            the status
	 */
	public WordCountContext(List<String> workers, String spoolOutDir,
			String outputDir, boolean isMap, WorkerStatus status)
	{
		this.workers = workers;
		if (workers != null)
		{
			this.workerCount = new BigInteger("" + workers.size());
		}
		this.spoolOutDir = spoolOutDir;
		this.outputDir = outputDir;
		this.isMap = isMap;
		this.status = status;
	}

	/* (non-Javadoc)
	 * @see edu.upenn.cis455.mapreduce.Context#write(java.lang.String, java.lang.String)
	 */
	@Override
	public void write(String key, String value)
	{
		if (isMap)
		{
			// get hash of key
			BigInteger hash;
			try
			{
				hash = hashKey(key);
				// find the worker based on hash
				int index = hash.multiply(workerCount).divide(max).intValue();
				// write to correct file
				FileWriter fileWriter = new FileWriter(spoolOutDir + "/"
						+ workers.get(index), true);
				fileWriter.append(key + "\t" + value + "\r\n");
				fileWriter.close();
				System.out.println("writing - " + key + "\t" + value + " to "
						+ spoolOutDir + "/" + workers.get(index));
				synchronized (status)
				{
					// update status for the ping thread
					int count = Integer.valueOf(status.getKeysWritten());
					count++;
					status.setKeysWritten("" + (count));
				}
			}
			catch (NoSuchAlgorithmException e)
			{
				System.out
						.println("worker context : NoSuchAlgorithmException while hashing key - "
								+ key);
				e.printStackTrace();
			}
			catch (IOException e)
			{
				System.out
						.println("worker context : IOException while writing key after map");
				e.printStackTrace();
			}
		}
		else
		{
			try
			{
				// write to correct file
				FileWriter fileWriter = new FileWriter(outputDir + "/output",
						true);
				fileWriter.append(key + "\t" + value + "\r\n");
				fileWriter.close();
				System.out.println("writing - " + key + "\t" + value + " to "
						+ outputDir + "/output");
			}
			catch (IOException e)
			{
				System.out
						.println("worker context : IOException while writing key after reduce");
				e.printStackTrace();
			}
		}

	}

	/**
	 * Hash key.
	 *
	 * @param key
	 *            the key
	 * @return the big integer
	 * @throws NoSuchAlgorithmException
	 *             the no such algorithm exception
	 */
	public BigInteger hashKey(String key) throws NoSuchAlgorithmException
	{
		MessageDigest encrypt = MessageDigest.getInstance("SHA-1");
		encrypt.reset();
		encrypt.update(key.getBytes());
		BigInteger result = new BigInteger(1, encrypt.digest());
		return result;
	}
}
