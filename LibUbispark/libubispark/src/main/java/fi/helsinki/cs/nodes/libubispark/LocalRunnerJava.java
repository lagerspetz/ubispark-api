package fi.helsinki.cs.nodes.libubispark;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinTask;

/**
 * Created by lagerspe on 17.2.2017.
 */

public class LocalRunnerJava {
    LocalRunner r = null;
    public LocalRunnerJava(int threads){
        r = new LocalRunner(threads);
    }

    public <T extends Object> ForkJoinTask scheduleTask(Callable<T> task){
        return r.scheduleTask(task);
    }
}
