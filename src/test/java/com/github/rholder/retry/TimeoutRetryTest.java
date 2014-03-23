package com.github.rholder.retry;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * TimeoutRetryTest
 */
public class TimeoutRetryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeoutRetryTest.class);

    private static final String TEST_STRING = "wibble";


    @Test
    public void callableReturnsString_allIsOk() {
        SimpleTimeLimiter simpleTimeLimiter = new SimpleTimeLimiter();

        Callable<String> successfulCallable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                return TEST_STRING;
            }
        };

        //test
        String returnString = null;
        try {
            returnString = simpleTimeLimiter.callWithTimeout(successfulCallable, 500, TimeUnit.MILLISECONDS, false);

        } catch (UncheckedTimeoutException ute) {
            LOGGER.error("UncheckedTimeoutException caught", ute);
        } catch (Exception e) {
            LOGGER.error("Raw Ex caught", e);
        }

        //verify
        assertThat(returnString, is(TEST_STRING));
    }


    @Test(expected = UncheckedTimeoutException.class)
    public void callableThreadSleepFor5secs_uncheckedTimeoutExThrown() throws Exception {

        Callable<String> sleepy5secCallable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    //do nothing
                }

                return null;
            }
        };

        SimpleTimeLimiter simpleTimeLimiter = new SimpleTimeLimiter();

        //test
        simpleTimeLimiter.callWithTimeout(sleepy5secCallable, 500, TimeUnit.MILLISECONDS, false);
    }


    @Test(expected = UncheckedTimeoutException.class)
    public void callableInfiniteLoop_uncheckedTimeoutExThrown() throws Exception {
        SimpleTimeLimiter simpleTimeLimiter = new SimpleTimeLimiter();

        Callable<String> infiniteLoop = new Callable<String>() {
            @Override
            public String call() throws Exception {
                int i = 0;
                while (i >= 0) {
                    //do nothing
                }

                return null;
            }
        };

        simpleTimeLimiter.callWithTimeout(infiniteLoop, 500, TimeUnit.MILLISECONDS, false);
    }


    @Test(expected = RetryException.class)
    public void callableInfiniteLoopWithRetry_retryExThrown() throws Exception {

        final Callable<String> infiniteLoopCallable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                long i = 0;
                while (i >= 0) {
                    //do nothing
                }
                return null;
            }
        };

        Callable<String> infiniteLoopWithTimeoutWillFail = new Callable<String>() {
            @Override
            public String call() throws Exception {
                SimpleTimeLimiter simpleTimeLimiter = new SimpleTimeLimiter();
                return simpleTimeLimiter.callWithTimeout(infiniteLoopCallable, 500, TimeUnit.MILLISECONDS, false);
            }
        };

        Retryer<String> retryer = RetryerBuilder.<String>newBuilder()
                .retryIfExceptionOfType(UncheckedTimeoutException.class)
                .retryIfRuntimeException()
                .withStopStrategy(StopStrategies.stopAfterAttempt(3))
                .build();

        //test
        //expected behaviour is timeout of infiniteLoopCallable, with 3 retries, and ultimately throws a RetryException
        retryer.call(infiniteLoopWithTimeoutWillFail);
    }

}
