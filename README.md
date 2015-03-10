# SerialAndParallelExecutor

The ParallelAndSerialExecutor is an executor which will run tasks in parallel or serially depending on what tasks are passed in.
To execute tasks serially, call {@link #execute(Runnable)} with a runnable that extends {@link SerialRunnable}.
The executor will guarantee execution of any runnables with the same identifier in a serial order.
All other tasks which do not implement the {@link SerialRunnable} will be executed in a parallel fashion.

# Serial Example:
```
getExecutor().execute(new ParallelAndSerialExecutor.SerialRunnable<String>() {
           @Override
           public String getIdentifier() {
               return storyId;
           }
           public void run() {
               //Your task to run
           }
       });
```    
#Parallel Example:
```
getExecutor().execute(new Runnable() {
               @Override
               public void run() {
                    //Your task to run
               }
           });
```