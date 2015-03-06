<div class="workshop-cta">
        <b>FREE WORKSHOP: Parallel Processing in Java</b>Marko Topolnik is giving a free virtual workshop faster parallel processing in Java.
        <a href="http://airpa.ir/tutorial-signup-parallel-data-java" class="trackPostCTA">>> Sign up to secure a spot</a>
</div>

## 1 Introduction

Lambdas, serious type inference, and the Streams API changed my Java code so deeply that it feels like a new language. I can't remember a single for-loop that I had to write with Java 8 (well, except within a Spliterator &mdash; more on that later). However, the API won't let you forget one thing: its first and foremost concern is parallelization. This entailed some tough, potentially dangerous compromises.

For example, we don't get a chunking operation (like `partition` in Clojure), `takeWhile`, `dropWhile`, or some others. None of these are omissions: several, in fact, existed in earlier phases of the API's development and were intentionally dropped due to serious difficulties in their parallel implementation. As a further example, we have to suffer through an extra level of complexity in the `Collector` API which is needed exclusively in parallel processing: the *combiner* function.

The API's usefulness for general-purpose collection processing is almost a side effect: it just happens that the programming paradigm essential for parallelization is the one which makes *any* processing model more convenient &mdash; namely, the clear separation of the mechanics of iterating from the logic of processing each element. I'll venture an even stronger claim: if it weren't for the specter of missing the boat to the next big thing (seamless multicore computing), concise lambda syntax with sophisticated type inference would have remained a non-goal for Java, a language proudly designated by its creator to be a ["*blue-collar language*."](http://dfjug.org/thefeelofjava.pdf)

No pain, no gain, they say &mdash; fair enough, so let's move on to the gain: the magic of seamless parallelization at work. This is where it gets awkward:

1. To start with, you need a substantial chunk of CPU-intensive processing, all happening on a single collection of data. Parallelization implies overheads (task coordination, handover from thread to thread, etc.) and won't help you to speed up all those little bits of processing here and there. Needless to say, tasks where anything but the CPU is the bottleneck will not benefit from parallelization.

2. It needs to be *stateless*: the processing of an element should not share any mutable state with the processing of any other element. Also, the processing of one item should not depend on the results of processing any other. Good parallelism asks for thread *isolation*, with the least possible amount of *coordination*.

3. The data should support low-latency random access. This is what the Fork/Join framework likes most: it wants to split your collection down the middle, then repeat that for each half, as many (and as few) times needed to get the least possible number of chunks required to saturate all CPU cores. This constraint leaves little choice besides storing everything in an on-heap array.

You could probably match points 1 and 2 above to a familiar example from a project of your own; point 3, however, spoils a serious share of parallelization opportunities. Our huge data resides on the file system, in a database, on the network... we have no need to hold on to it in RAM for longer than the processing of a single client request. There are a few specialized areas where we do: physical simulations (games included) and similar areas where a large model is kept for prolonged periods and being iteratively operated on. Those areas, however, are not Java's strong suit: they are more likely to belong to the realms of Fortran, C/C++, or some esoteric language specific to its narrow area of computing.

We could, of course, load our dataset into RAM just before the one-shot processing begins, and discard it immediately after. However:

1. This is not called for by the processing model itself: as already explained, the best parallelization targets are those with no interdependencies between items, so while one item is loaded and being processed, no other item's presence is needed.

2. It introduces delays: the CPU sits mostly idle while data is being loaded.

3. Worst of all, it turns an essentially O(1)-space problem into O(N), destroying scalability.

The `Stream` paradigm, just like `Iterable`, is perfectly able to support the idea of *laziness*: items in it don't need to be materialized until needed, and each item is encountered exactly once so it doesn't need to be retained. The way to parallelize a lazy sequential (non-random-access) stream is actually quite simple and well-known: group consecutive items into *batches* of sufficient size to require palpable processing time (on the order of 1-10 milliseconds), queue up those batches, and let each thread take a batch when ready. The underlying assumption, of course, is that I/O speed overtakes processing speed; otherwise the parallelization opportunity wouldn't be there to begin with.

It sounds obvious that the requirement to have everything in RAM should not exist and that the implementation should respect this essential use case. Indeed, specific work has been done to support it, as evidenced in the implementation of the [`ForEachTask::compute` method](http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/8-b132/java/util/stream/ForEachOps.java#ForEachOps.ForEachTask.compute%28%29). A bit of explanation is present in [this message by Paul Sandoz on the lambda-dev list](http://mail.openjdk.java.net/pipermail/lambda-dev/2014-March/011976.html). 

Still, the message is clear that the focus of the API is squarely on random-access collections of known size, and the use case of I/O-based streams is described as "a worst-case scenario of a sequentially ordered source of unknown size." I cannot suppress my doubt in the sanity of an implementation which fears such a major use case as its worst nightmare. 

So, are we actually faced with a "pain, with no gain" situation?

Contrary to the bleak statements by the core Java team, I found their implementation to be quite able to support the parallelization of I/O-based sources and have yet to experience any kind of trouble with it. I did need to write some custom code to enforce the appropriate batching policy: ideally this should have been provided by the JDK, and maybe it will be in a future release.

## 2 Solution: Fixed batch size streams

Our goal is to provide ready-made support to implement an I/O-based stream source, relieving the user from the pitfalls of proper implementation of the batching behavior as needed for efficient parallelization. The user will be required to understand his computation task only as much as needed to supply a single parameter: the batch size. 

This should be such that the single-threaded processing of the batch takes about 1 to 10 millseconds. Since this is obviously specific to each processing task, batch size must be considered separately for each usage. However, the range of acceptable processing times is quite large so often a single value will work for many tasks.

The cornerstone for integrating custom I/O sources into the Streams API is the `Spliterator` interface. One incredibly helpful thing about spliterators is that they *do not need to be thread-safe*. The framework guarantees that it will use your spliterator a single thread at a time, handing it over to the next thread in a clean, thread-safe manner. This turns the implementation of an I/O-based spliterator from a near impossibility to a simple and straightforward task.

The JDK already provides an abstract spliterator class with exactly the goals described above. Its batching policy is hard-coded, however: the size of the first batch is 1024, and it increases by 1024 for each subsequent batch. In other words, the batch sizes follow an arithmetic progression. The goal was to try to cater to as many use cases as possible with a non-configurable policy, but priority was given to tasks which take a very short time per item (such tasks need a large batch). 

This policy fails badly for tasks where the size of the input is moderate (for example, 10,000 items will be split into just four batches), but each item takes substantial time to process (where "substantial" is anything above 1 millisecond). Again, something which I see as a rather typical case in my field of experience is the worst-case scenario for the JDK design. 

### 2.1 The Spliterator interface

Considered in full, the Spliterator API isn't exactly trivial and some introduction is due; however, we shall see that it allows us to provide a generic implementation for all but a single method &mdash; the one that consumes the I/O source in question. 

The name "spliterator" is a portmanteau of the two main concerns this abstraction is designed to handle: *splitting* and *iterating*. These are the two corresponding methods:

<!--code lang=java-->

    Spliterator<T> trySplit();
    boolean tryAdvance(Consumer<? super T> action);


#### 2.1.1 trySplit()

`trySplit()` splits off a part of the sequence *this* spliterator was supposed to handle into a new spliterator. In our case this must always be the strict prefix of *this* spliterator's sequence, because the I/O resource behind it stays with *this* and the returned spliterator contains the starting batch in materialized form. A useful, reusable implementation of this method is the goal of our support class.

#### 2.1.2 tryAdvance(action), forEachRemaining(action)

`tryAdvance()` is where the actual stream processing takes place: the spliterator advances by one item and invokes the passed-in `action` on it. This is the method where the user must supply behavior appropriate to his I/O source. 

It has a cousin called `forEachRemaining()`, which is there purely for optimization purposes: when the execution framework decides to consume a spliterator in full, it can make a single call to `forEachRemaining()` instead of calling `tryAdvance()` for each item individually. This method has a default implementation which repeatedly calls `tryAdvance()`, so there is no rush in implementing it ourselves. Often, though, it is quite easy to provide an optimized implementation. 

#### 2.1.3 characteristics(), hasCharacteristics(characteristics)

Next we have the `int characteristics()` method, which is again devoted to various aspects of optimization. Spliterator reports its characteristics in a single `int`, treated as a bitfield. Normally this is considered an anti-pattern because an `EnumSet` is the recommended typesafe alternative, but once again optimization concerns trump everything else in this API. For our usage only a few spliterator characteristics are relevant:

- `Spliterator.SUBSIZED` guarantees that each spliterator returned by `trySplit()` will itself be both `SIZED` (its item count will be precisely known) and `SUBSIZED`. This characteristic will be guaranteed by our general class because we materialize the items in advance when splitting and thus know their exact number.

- `Spliterator.ORDERED` signals that the spliterator will respect a well-defined *encounter order*: items will be visited by it in this order and `trySplit()` will always return a strict prefix with respect to it. Most of the time, we will want to enable this characteristic since the nature of the stream source forces us to respect it in any case. Not specifying `ORDERED` may allow more optimization inside some operations, but it should be handled with a lot of care as it invites very confusing bugs: non-commutative operations will not be required to respect the encounter order in that case.

- `Spliterator.NONNULL` indicates that no item will ever be `null`. Many kinds of processing get much more involved in the face of possible `null` values, so setting 
(and respecting) this characteristic makes life easier for everyone.

- `Spliterator.IMMUTABLE` guarantees that the source cannot be structurally modified (items added, removed, or replaced). It is hard to imagine how this could be violated for a lazy stream, where items don't even exist before they are used. There is a closely related method &mdash; `hasCharacteristics(int characteristics)` &mdash; but it has a default implementation and there is little reason to ever override it.

#### 2.1.4 estimateSize()

Further, we have `long estimateSize()`. Since we almost never know the size of our I/O-based source in advance, normally this will always return `Long.MAX_VALUE` to signal "size unknown". The main usage of this method by the stream execution engine is to decide whether to use `trySplit()` or just process the entire spliterator in a single thread, so it would only be of interest if you know that the size is quite small. 

Remember this is just a size *estimate* and tolerates imprecision. A related method is `long getExactSizeIfKnown()` which is required to give the exact size or `-1L` if not known. The default implementation takes care of this method's semantics perfectly (relying on the `SIZED` characteristic).

#### 2.1.5 getComparator()

Finally, there is the `Comparator<? super T> getComparator()` method, which will be of little interest to us because it is only involved in spliterators with the `SORTED` characteristic, and that is almost never the case with I/O-based spliterators.

### 2.2 Code: FixedBatchSpliteratorBase

Let me now present the abstract class that takes care of the splitting aspect, leaving just the specifics of iterating through a given I/O source to the implementor.

<!--code lang=java linenums=true-->

    import static java.util.Spliterators.spliterator;
    
    import java.util.Comparator;
    import java.util.Spliterator;
    import java.util.function.Consumer;

    public abstract class FixedBatchSpliteratorBase<T> implements Spliterator<T> {
      private final int batchSize;
      private final int characteristics;
      private long est;
    
      public FixedBatchSpliteratorBase(int characteristics, int batchSize, long est) {
        this.characteristics = characteristics | SUBSIZED;
        this.batchSize = batchSize;
        this.est = est;
      }
      public FixedBatchSpliteratorBase(int characteristics, int batchSize) {
        this(characteristics, batchSize, Long.MAX_VALUE);
      }
      public FixedBatchSpliteratorBase(int characteristics) {
        this(characteristics, 128, Long.MAX_VALUE);
      }
      public FixedBatchSpliteratorBase() {
        this(IMMUTABLE | ORDERED | NONNULL);
      }

      @Override public Spliterator<T> trySplit() {
        final HoldingConsumer<T> holder = new HoldingConsumer<>();
        if (!tryAdvance(holder)) return null;
        final Object[] a = new Object[batchSize];
        int j = 0;
        do a[j] = holder.value; while (++j < batchSize && tryAdvance(holder));
        if (est != Long.MAX_VALUE) est -= j;
        return spliterator(a, 0, j, characteristics() | SIZED);
      }
      @Override public Comparator<? super T> getComparator() {
        if (hasCharacteristics(SORTED)) return null;
        throw new IllegalStateException();
      }
      @Override public long estimateSize() { return est; }
      @Override public int characteristics() { return characteristics; }
    
      static final class HoldingConsumer<T> implements Consumer<T> {
        Object value;
        @Override public void accept(T value) { this.value = value; }
      }
    }

As expected, most of the action goes on inside `trySplit()`, an adaptation of the JDK-provided `AbstractSpliterator#trySplit()`. We use `tryAdvance()` with our own `Consumer` implementation to extract the data from the stream. This way we avoid creating any extra demand on the implementor, who is still required to implement just `tryAdvance()`. 

A neat thing about this design is that it will incur almost no overhead: the `HoldingConsumer` instance is a perfect target for Escape Analysis, so the `value` field will most likely end up directly on the call stack, as if it was a local variable. Moving on, we store the collected items into an array and create an `ArraySpliterator` by delegatig to the JDK method that does this.

This is how I used `FixedBatchSpliteratorBase` to implement a stream on top of opencsv's CSV parser:

<!--code lang=java linenums=true-->

    public class CsvSpliterator extends FixedBatchSpliteratorBase<String[]> {
        private final CSVReader cr;

        CsvSpliterator(CSVReader cr, int batchSize) {
          super(IMMUTABLE | ORDERED | NONNULL, batchSize);
          if (cr == null) throw new NullPointerException("CSVReader is null");
          this.cr = cr;
        }
        public CsvSpliterator(CSVReader cr) { this(cr, 128); }

        @Override public boolean tryAdvance(Consumer<? super String[]> action) {
          if (action == null) throw new NullPointerException();
          try {
            final String[] row = cr.readNext();
            if (row == null) return false;
            action.accept(row);
            return true;
          } catch (RuntimeException e) {  
            throw e; 
          } catch (Exception e) { 
            throw new RuntimeException(e); 
          }
        }

        @Override public void forEachRemaining(Consumer<? super String[]> action) {
          if (action == null) throw new NullPointerException();
          try {
            for (String[] row; (row = cr.readNext()) != null;) action.accept(row);
          } catch (RuntimeException e) {  
            throw e; 
          } catch (Exception e) { 
            throw new RuntimeException(e); 
          }
        }
      }
    }

As you can see, except for the drudgery of exception translation and null-checking, both `forEachRemaining` and `tryAdvance` are quite trivial and obvious methods. The class adds a couple of constructors and that's it. You'll want to accompany each spliterator implementation with convenient stream factory methods, for example:

<!--code lang=java linenums=true-->

    public static Stream<String[]> csvStream(InputStream in) {
      final CSVReader cr = new CSVReader(new InputStreamReader(in));
      return StreamSupport.stream(new CsvSpliterator(cr), false).onClose(() -> {
        try { cr.close(); } catch (IOException e) { throw new UncheckedIOException(e); }
      });
    }

Note the key novelty here: the `onClose()` callback, propagating a stream closure event to the underlying I/O resource. This isn't as essential as it may seem because we can often surround the invocation of this factory together with the stream processing code into a single *try with resources* block, thus avoiding any risk of resource leakage. However, in certain more elaborate scenarios there may be one layer which provides the stream and another which processes it: then this does become essential. `Stream` itself is `AutoCloseable` and can be used on its own in a *try with resources*.

Here's an example of simple usage (this one doesn't rely on `onClose()`):

<!--code lang=java linenums=true-->

    try (FileInputStream fis = new FileInputStream("data.csv")) {
      csvStream(fis).parallel()
        .map(fields -> encrypt(String.join("$$", fields)))
        .forEach(System.out::println);
    }

or, alternatively (relying on `onClose()` to close the input stream),

<!--code lang=java linenums=true-->

    try (Stream<String[]> stream = csvStream(new FileInputStream("data.csv"))) {
      stream.parallel()
        .map(fields -> encrypt(String.join("$$", fields)))
        .forEach(System.out::println);
    }


As for the processing itself, there is nothing special to see &mdash; once the spliterator business is settled, a stream is a stream is a stream.

## 3 Validation

OK, so our stream can talk the talk, but can it walk the walk? How does all of the above translate into measurable performance? At this point we demand a piece of code which can reproducibly demonstrate the reality of the above claims. 

To keep it as simple as possible, we shall make use of the JDK-provided stream over the lines of a text file &mdash; `Files.lines()`. The example contributes another useful piece of code: a fixed-batch Spliterator *wrapper*, which can turn any existing spliterator into a fixed-batch one. Further, thanks to the fact that the `Stream` interface includes the `spliterator()` method, it can also turn any *stream* you have into a stream with fixed batch size. The class that does this is a trivial implementation of the `FixedBatchSpliteratorBase`:

<!--code lang=java linenums=true-->

    import static java.util.stream.StreamSupport.stream;

    import java.util.Spliterator;
    import java.util.function.Consumer;
    import java.util.stream.Stream;

    public class FixedBatchSpliterator<T> extends FixedBatchSpliteratorBase<T> {
      private final Spliterator<T> spliterator;

      public FixedBatchSpliterator(Spliterator<T> toWrap, int batchSize) {
        super(toWrap.characteristics(), batchSize, toWrap.estimateSize());
        this.spliterator = toWrap;
      }

      public static <T> FixedBatchSpliterator<T> batchedSpliterator(Spliterator<T> toWrap, int batchSize) {
        return new FixedBatchSpliterator<>(toWrap, batchSize);
      }

      public static <T> Stream<T> withBatchSize(Stream<T> in, int batchSize) {
        return stream(batchedSpliterator(in.spliterator(), batchSize), true);
      }

      @Override public boolean tryAdvance(Consumer<? super T> action) {
        return spliterator.tryAdvance(action);
      }
      @Override public void forEachRemaining(Consumer<? super T> action) {
        spliterator.forEachRemaining(action);
      }
    }


We employ this class in the following benchmark, which makes a head-to-head comparison of the default JDK spliterator with ours. These are the steps taken by the code:

1. Generate an input file consisting of 6,000 lines filled with some arbitrary characters.
2. Create a stream over the file.
3. Do some floating-point calculations with the characters of each line.
4. Write the result of the calculation to a global variable (just to consume the value and prevent any dead code elimination).
5. Return the running time of the calculation as its result.
6. Sum up all the running times to find out the total CPU time consumed.
7. Compare that sum with the wall-clock duration of the benchmark.

<!--code lang=java linenums=true-->

    import static java.util.concurrent.TimeUnit.SECONDS;
    import static test.FixedBatchSpliterator.withBatchSize;

    import java.io.IOException;
    import java.io.PrintWriter;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.nio.file.Paths;
    import java.util.stream.Stream;

    public class SpliteratorBenchmark
    {
      static double sink;

      public static void main(String[] args) throws IOException {
        final Path inputPath = createInput();
        for (int i = 0; i < 3; i++) {
          System.out.println("Start processing JDK stream");
          measureProcessing(Files.lines(inputPath));
          System.out.println("Start processing fixed-batch stream");
          measureProcessing(withBatchSize(Files.lines(inputPath), 10));
        }
      }

      private static void measureProcessing(Stream<String> input) throws IOException {
        final long start = System.nanoTime();
        try (Stream<String> lines = input) {
          final long totalTime = lines.parallel()
            .mapToLong(SpliteratorBenchmark::processLine).sum();
          final double cpuTime = totalTime, realTime = System.nanoTime()-start;
          final int cores = Runtime.getRuntime().availableProcessors();
          System.out.println("          Cores: " + cores);
          System.out.format("       CPU time: %.2f s\n", cpuTime/SECONDS.toNanos(1));
          System.out.format("      Real time: %.2f s\n", realTime/SECONDS.toNanos(1));
          System.out.format("CPU utilization: %.2f%%\n\n", 100.0*cpuTime/realTime/cores);
        }
      }

      private static long processLine(String line) {
        final long localStart = System.nanoTime();
        double d = 0;
        for (int i = 0; i < line.length(); i++)
          for (int j = 0; j < line.length(); j++)
            d += Math.pow(line.charAt(i), line.charAt(j)/32.0);
        sink += d;
        return System.nanoTime()-localStart;
      }

      private static Path createInput() throws IOException {
        final Path inputPath = Paths.get("input.txt");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(inputPath))) {
          for (int i = 0; i < 6_000; i++) {
            final String text = String.valueOf(System.nanoTime());
            for (int j = 0; j < 15; j++) w.print(text);
            w.println();
          }
        }
        return inputPath;
      }
    }

A typical output produced on my machine looks like this:

<!-- code lang=bash linenums=true -->

    Start processing JDK stream
    Cores: 4
    CPU time: 12.46 s
    Real time: 6.00 s
    CPU utilization: 51.90%
    
    Start processing fixed-batch stream
    Cores: 4
    CPU time: 13.79 s
    Real time: 3.48 s
    CPU utilization: 99.08%

The JDK spliterator initially gives three of my four cores something to do, but soon the first core is done with its batch of size 1024; after that, only 50% of the CPU is being utilized. This then drops to just 25% as the last core is left to finish its batch alone. 

The fixed-batch spliterator, on the other hand, generates 600 batches of size 10, which are evenly assigned to the cores as they become available.

## 4 Conclusion

I have deployed a CSV spliterator similar to the one presented in this article to a production application. Each line of CSV entails substantial work with a Lucene index, plus further custom processing. It is eating through 100 MB-sized CSVs, burning a four-core CPU at 100% utilization for 45 minutes at a time. 

I get clean, linear 4x speedup compared to single-threaded processing. I have tried to find a weakness, such as stalled input, some items taking exceedingly long to process, or suddenly closing the input stream, but I am still at a loss as to which exactly scenario makes this fail. I have even tried out a scenario where one request completely hangs and there are other requests, again the framework stands up to the task and allows the other request to proceed. 

I trust that the core Java experts know what they are talking about when warning me about this use case, but I have not received any *specific* information about a pitfall. Thus I feel that the situation has both an encouraging and a worrying aspect to it: I have substantial proof that things work out in many scenarios, yet couch a latent fear of some major breakage still lurking. 

I invite you, the reader, to not just try this out, but beat it with everything you've got, and report back if you find a reproducible pitfall. If you do come up with anything, or would like to discuss similar techniques, be sure to book me for an AirPair!