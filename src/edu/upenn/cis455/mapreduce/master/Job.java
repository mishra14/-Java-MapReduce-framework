package edu.upenn.cis455.mapreduce.master;

import java.util.List;

import edu.upenn.cis455.mapreduce.master.WorkerStatus.statusType;

/**
 * This class is a bean class to hold all the information about the job created
 * by the user from the UI.
 * 
 * pageContent.append("creating new job with following params - <br>
 * " + "Job : " + request.getParameter("job") + "<br>
 * " + "Input Directory : " + request.getParameter("inputdir") + "<br>
 * " + "Output Directory : " + request.getParameter("outputdir") + "<br>
 * " + "Map Threads : " + request.getParameter("mapthreads") + "<br>
 * " + "Reduce Threads : " + request.getParameter("reducethreads") + "<br>
 * ");
 * 
 * @author cis455
 *
 */
public class Job
{

	/** The job name. */
	private String jobName;

	/** The input directory. */
	private String inputDirectory;

	/** The output directory. */
	private String outputDirectory;

	/** The map threads. */
	private String mapThreads;

	/** The reduce threads. */
	private String reduceThreads;

	/** The workers. */
	private List<String> workers;

	/** The status. */
	private statusType status;

	/**
	 * Instantiates a new job.
	 *
	 * @param jobName
	 *            the job name
	 * @param inputDirectory
	 *            the input directory
	 * @param outputDirectory
	 *            the output directory
	 * @param mapThreads
	 *            the map threads
	 * @param reduceThreads
	 *            the reduce threads
	 */
	public Job(String jobName, String inputDirectory, String outputDirectory,
			String mapThreads, String reduceThreads)
	{
		super();
		this.jobName = jobName;
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
		this.mapThreads = mapThreads;
		this.reduceThreads = reduceThreads;
		this.workers = null;
		this.status = statusType.idle;
	}

	/**
	 * Gets the job name.
	 *
	 * @return the job name
	 */
	public String getJobName()
	{
		return jobName;
	}

	/**
	 * Sets the job name.
	 *
	 * @param jobName
	 *            the new job name
	 */
	public void setJobName(String jobName)
	{
		this.jobName = jobName;
	}

	/**
	 * Gets the input directory.
	 *
	 * @return the input directory
	 */
	public String getInputDirectory()
	{
		return inputDirectory;
	}

	/**
	 * Sets the input directory.
	 *
	 * @param inputDirectory
	 *            the new input directory
	 */
	public void setInputDirectory(String inputDirectory)
	{
		this.inputDirectory = inputDirectory;
	}

	/**
	 * Gets the output directory.
	 *
	 * @return the output directory
	 */
	public String getOutputDirectory()
	{
		return outputDirectory;
	}

	/**
	 * Sets the output directory.
	 *
	 * @param outputDirectory
	 *            the new output directory
	 */
	public void setOutputDirectory(String outputDirectory)
	{
		this.outputDirectory = outputDirectory;
	}

	/**
	 * Gets the map threads.
	 *
	 * @return the map threads
	 */
	public String getMapThreads()
	{
		return mapThreads;
	}

	/**
	 * Sets the map threads.
	 *
	 * @param mapThreads
	 *            the new map threads
	 */
	public void setMapThreads(String mapThreads)
	{
		this.mapThreads = mapThreads;
	}

	/**
	 * Gets the reduce threads.
	 *
	 * @return the reduce threads
	 */
	public String getReduceThreads()
	{
		return reduceThreads;
	}

	/**
	 * Sets the reduce threads.
	 *
	 * @param reduceThreads
	 *            the new reduce threads
	 */
	public void setReduceThreads(String reduceThreads)
	{
		this.reduceThreads = reduceThreads;
	}

	/**
	 * Gets the workers.
	 *
	 * @return the workers
	 */
	public List<String> getWorkers()
	{
		return workers;
	}

	/**
	 * Sets the workers.
	 *
	 * @param workers
	 *            the new workers
	 */
	public void setWorkers(List<String> workers)
	{
		this.workers = workers;
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "Job [jobName=" + jobName + ", inputDirectory=" + inputDirectory
				+ ", outputDirectory=" + outputDirectory + ", mapThreads="
				+ mapThreads + ", reduceThreads=" + reduceThreads
				+ ", workers=" + workers + ", status=" + status + "]";
	}

}
