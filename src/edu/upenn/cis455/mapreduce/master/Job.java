package edu.upenn.cis455.mapreduce.master;

import java.util.ArrayList;
import java.util.List;

/**
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

	@Override
	public String toString()
	{
		return "Job [jobName=" + jobName + ", inputDirectory=" + inputDirectory
				+ ", outputDirectory=" + outputDirectory + ", mapThreads="
				+ mapThreads + ", reduceThreads=" + reduceThreads
				+ ", workers=" + workers + "]";
	}

}
