using System.Diagnostics;

namespace GraphDatabaseTryout.Performance
{
    internal class PerformanceCounter : IDisposable
    {
        private static ActivitySource activitySource = new ActivitySource("Test.Performance");
        private Activity? activity;

        static PerformanceCounter()
        {
            ActivitySource.AddActivityListener(new ActivityListener()
            {
                ShouldListenTo = (source) => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity => Console.WriteLine("Started: {0,-15} {1,-60}", activity.OperationName, activity.Id),
                ActivityStopped = activity => Console.WriteLine("Stopped: {0,-15} {1,-60} {2,-15}", activity.OperationName, activity.Id, activity.Duration)
            });
        }

        public PerformanceCounter(string name)
        {
            activity = activitySource.StartActivity(name);
        }

        public void Dispose()
        {
            activity?.Dispose();
        }
    }
}
