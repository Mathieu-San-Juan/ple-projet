package bigdata;

public class Activity {
    long startDate;
    long endDate;
    long duration;
    String patterns[];
    int nPatterns;
    String jobs[];
    int nJobs;
    String days[];
    int nDays;

    public Activity(long startDate, long endDate, long duration, String patterns[], int nPatterns, String jobs[], int nJobs, String days[],int nDays) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.duration = duration;
        this.patterns = patterns;
        this.nPatterns = nPatterns;
        this.jobs = jobs;
        this.nJobs = nJobs;
        this.days = days;
        this.nDays = nDays;
    }
}