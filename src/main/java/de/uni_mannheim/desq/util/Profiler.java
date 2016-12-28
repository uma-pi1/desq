package de.uni_mannheim.desq.util;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

/** Profiles wall-clock time and memory consumption. Note that using this profiler may significantly affect running
 * times, i.e., it should not be used in production.
 *
 * Created by rgemulla on 7.12.2016.
 */
public class Profiler {
    public Stopwatch stopwatch = new Stopwatch();
    public long usedMemory = 0;

    public static long getUsedMemory() {
        // slow but quite effective way to determine used memory as accurately as possible
        System.gc();
        System.runFinalization();
        System.gc();
        System.runFinalization();
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    public void start() {
        usedMemory = getUsedMemory();
        stopwatch.reset();
        stopwatch.start();
    }

    public void stop() {
        stopwatch.stop();
        usedMemory = getUsedMemory() - usedMemory;
    }

    @Override
    public String toString() {
        return stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms"
            + " (" + (usedMemory >= 0 ? "+" : "") + (usedMemory/1024) + "kB)";
    }
}
