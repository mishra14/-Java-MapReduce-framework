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
	private String jobName;
	private String inputDirectory;
	private String outputDirectory;
	private String mapThreads;
	private String reduceThreads;
	private List<String> workers;
	private statusType status;

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

	public String getJobName()
	{
		return jobName;
	}

	public void setJobName(String jobName)
	{
		this.jobName = jobName;
	}

	public String getInputDirectory()
	{
		return inputDirectory;
	}

	public void setInputDirectory(String inputDirectory)
	{
		this.inputDirectory = inputDirectory;
	}

	public String getOutputDirectory()
	{
		return outputDirectory;
	}

	public void setOutputDirectory(String outputDirectory)
	{
		this.outputDirectory = outputDirectory;
	}

	public String getMapThreads()
	{
		return mapThreads;
	}

	public void setMapThreads(String mapThreads)
	{
		this.mapThreads = mapThreads;
	}

	public String getReduceThreads()
	{
		return reduceThreads;
	}

	public void setReduceThreads(String reduceThreads)
	{
		this.reduceThreads = reduceThreads;
	}

	public List<String> getWorkers()
	{
		return workers;
	}

	public void setWorkers(List<String> workers)
	{
		this.workers = workers;
	}

	public statusType getStatus()
	{
		return status;
	}

	public void setStatus(statusType status)
	{
		this.status = status;
	}

	@Override
	public String toString()
	{
		return "Job [jobName=" + jobName + ", inputDirectory=" + inputDirectory
				+ ", outputDirectory=" + outputDirectory + ", mapThreads="
				+ mapThreads + ", reduceThreads=" + reduceThreads
				+ ", workers=" + workers + ", status=" + status + "]";
	}

}
