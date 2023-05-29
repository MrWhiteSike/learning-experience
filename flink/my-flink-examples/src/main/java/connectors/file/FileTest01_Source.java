package connectors.file;

import entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.compactor.SimpleStringDecoder;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.File;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * File Source 是基于 Source API 同时支持批模式和流模式文件读取的统一 Source。
 * File Source 分为以下两个部分：SplitEnumerator 和 SourceReader。
 *
 * SplitEnumerator 负责发现和识别需要读取的文件，并将这些文件分配给 SourceReader 进行读取。
 * SourceReader 请求需要处理的文件，并从文件系统中读取该文件。
 * 可能需要指定某种 format 与 File Source 联合进行解析 CSV、解码AVRO、或者读取 Parquet 列式文件。
 *
 *
 * 有界流和无界流 #
 * 有界的 File Source（通过 SplitEnumerator）列出所有文件（一个过滤出隐藏文件的递归目录列表）并读取。
 *
 * 无界的 File Source 由配置定期扫描文件的 enumerator 创建。 在无界的情况下，SplitEnumerator 将像有界的 File Source 一样列出所有文件，
 * 但是不同的是，经过一个时间间隔之后，重复上述操作。
 * 对于每一次列举操作，SplitEnumerator 会过滤掉之前已经检测过的文件，将新扫描到的文件发送给 SourceReader。
 *
 *
 * Format Types #
 * 通过 file formats 定义的文件 readers 读取每个文件。 其中定义了解析和读取文件内容的逻辑。Source 支持多个解析类。
 * 这些接口是实现简单性和灵活性/效率之间的折衷。
 *
 * StreamFormat 从文件流中读取文件内容。它是最简单的格式实现， 并且提供了许多拆箱即用的特性（如 Checkpoint 逻辑），
 * 但是限制了可应用的优化（例如对象重用，批处理等等）。
 *
 * BulkFormat 从文件中一次读取一批记录。 它虽然是最 “底层” 的格式实现，但是提供了优化实现的最大灵活性。
 *
 */

public class FileTest01_Source {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(30000L);
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/Baisike/opensource/checkpoint");

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.minutes(5)));

        // 1. 创建Format类型对象，根据使用 Jackson 库的 SomePojo 的字段自动生成的
        // （注意：需要添加 @JsonPropertyOrder({field1, field2, ...}) 这个注释到自定义的类上，
        // 并且字段顺序与CSV文件列的顺序完全匹配）
        CsvReaderFormat<SensorReading> csvReaderFormat = CsvReaderFormat.forPojo(SensorReading.class);

        /**
         *  如果需要对CSV模式或解析选项进行更细粒度的控制，可以使用CsvReaderFormat的更底层的forSchema静态工厂方法。
         *
         */

        // 2. 创建File Source对象
        /**
         * 对于有界/批的使用场景，File Source 需要处理给定路径下的所有文件。 对于无界/流的使用场景，File Source 会定期检查路径下的新文件并读取。
         *
         * 当创建一个 File Source 时（通过上述任意方法创建的 FileSource.FileSourceBuilder），
         * 默认情况下，Source 为有界/批的模式。
         * 可以调用 AbstractFileSource.AbstractFileSourceBuilder.monitorContinuously(Duration) 设置 Source 为持续的流模式。
         */
        FileSource<SensorReading> fileSource = FileSource.forRecordStreamFormat(
                csvReaderFormat,
                Path.fromLocalFile(new File("C:\\Users\\Baisike\\opensource\\my-flink-examples\\src\\main\\resources\\input")))
                // 调用monitorContinuously这个方法时，就会设置source为持续的流模式。一段时间后重复扫描
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        // 3. fromSource生成 Stream Source
        DataStreamSource<SensorReading> streamSource = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "file source");

        streamSource.print("file");


        /**
         * File Sink
         * File Sink 将传入的数据写入存储桶中。考虑到输入流可以是无界的，每个桶中的数据被组织成有限大小的Part文件。
         * 完全可以配置为基于时间的方式往桶中写入数据，比如可以设置每个小时的数据写入一个新桶。意味着桶中将包含一个小时间隔内接收到的记录。
         *
         * 桶目录中的数据被拆分成多个part文件，对应接收数据桶的sink的每个subtask。
         * 每个桶中至少包含一个part文件，即sink只有一个subtask，并行度为1的情况。
         *
         * 根据配置滚动策略来创建park文件：
         * Row-encoded Formats：默认的策略是根据part文件大小进行滚动，还需要指定文件打开状态最长时间的超时以及文件关闭后的非活动状态的时间
         *
         *  Bulk-encoded Formats：每次创建checkpoint时进行滚动，并且用户也可以添加基于大小或者时间等的其他条件。
         *
         * 注意：在 STREAMING 模式下使用 FileSink 需要开启 Checkpoint 功能。
         *      文件只在 Checkpoint 成功时生成。如果没有开启 Checkpoint 功能，文件将永远停留在 in-progress 或者 pending 的状态，
         *      并且下游系统将不能安全读取该文件数据。
         *
         *
         *
         * Format Types #
         * FileSink 不仅支持 Row-encoded 也支持 Bulk-encoded，例如 Apache Parquet。 这两种格式可以通过如下的静态方法进行构造：
         *
         * Row-encoded sink: FileSink.forRowFormat(basePath, rowEncoder)
         * Bulk-encoded sink: FileSink.forBulkFormat(basePath, bulkWriterFactory)
         *
         *
         * Row-encoded Formats #
         * Row-encoded Format 需要指定一个 Encoder，在输出数据到文件过程中被用来将单个行数据序列化为 OutputStream。
         *
         * 除了 bucket assigner，RowFormatBuilder 还允许用户指定以下属性：
         *
         * Custom RollingPolicy ：自定义滚动策略覆盖 DefaultRollingPolicy
         * bucketCheckInterval (默认值 = 1 min) ：基于滚动策略设置的检查时间间隔
         *
         *
         *
         * Bulk-encoded Formats #
         * Bulk-encoded 的 Sink 的创建和 Row-encoded 的相似，但不需要指定 Encoder，而是需要指定 BulkWriter.Factory
         *
         *  BulkWriter 定义了如何添加和刷新新数据以及如何最终确定一批记录使用哪种编码字符集的逻辑。
         *
         * Flink 内置了5种 BulkWriter 工厂类：
         *
         * ParquetWriterFactory
         * AvroWriterFactory
         * SequenceFileWriterFactory
         * CompressWriterFactory
         * OrcBulkWriterFactory
         *
         * 注意： Bulk-encoded Format 仅支持一种继承了 CheckpointRollingPolicy 类的滚动策略。
         * 在每个 Checkpoint 都会滚动。另外也可以根据大小或处理时间进行滚动。
         *
         * Parquet Format：
         *  1.Flink 内置了为 Avro Format 数据创建 Parquet 写入工厂的快捷方法。在 AvroParquetWriters 类中可以发现那些方法以及相关的使用说明。
         *  2.Protobuf 数据 ：ParquetProtoWriters
         * 在程序中使用 Parquet 的 Bulk-encoded Format，需要添加如下依赖：
         * <dependency>
         *     <groupId>org.apache.flink</groupId>
         *     <artifactId>flink-parquet_2.12</artifactId>
         *     <version>1.15.2</version>
         * </dependency>
         *
         *
         * Avro Format：
         * Flink 也支持写入数据到 Avro Format 文件。在 AvroWriters 类中可以发现一系列创建 Avro writer 工厂的便利方法及其相关说明
         * 在程序中使用 AvroWriters，添加依赖：
         * <dependency>
         *     <groupId>org.apache.flink</groupId>
         *     <artifactId>flink-avro</artifactId>
         *     <version>1.15.0</version>
         * </dependency>
         *
         * 对于自定义创建的 Avro writers，例如，支持压缩功能，用户需要创建 AvroWriterFactory 并且自定义实现 AvroBuilder 接口
         *
         *
         * ORC Format：
         * ORC Format 的数据采用 Bulk-encoded Format，Flink 提供了 Vectorizer 接口的具体实现类 OrcBulkWriterFactory。
         * 像其他列格式一样也是采用 Bulk-encoded Format，Flink 中 OrcBulkWriter 是使用 ORC 的 VectorizedRowBatch 实现批的方式输出数据的。
         * 此方法中提供了用户直接使用的 VectorizedRowBatch 类的实例，因此，用户不得不编写从输入 element 到 ColumnVectors 的转换逻辑，
         * 然后设置在 VectorizedRowBatch 实例中。
         *
         * 例如：
         * Person类型的输入元素
         * class Person {
         *     private final String name;
         *     private final int age;
         *     ...
         * }
         * 转换 Person 类型元素的实现并在 VectorizedRowBatch 中设置：
         * import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
         * import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
         *
         * import java.io.IOException;
         * import java.io.Serializable;
         * import java.nio.charset.StandardCharsets;
         *
         * public class PersonVectorizer extends Vectorizer<Person> implements Serializable {
         * 	public PersonVectorizer(String schema) {
         * 		super(schema);
         *        }
         *    @Override
         *    public void vectorize(Person element, VectorizedRowBatch batch) throws IOException {
         * 		BytesColumnVector nameColVector = (BytesColumnVector) batch.cols[0];
         * 		LongColumnVector ageColVector = (LongColumnVector) batch.cols[1];
         * 		int row = batch.size++;
         * 		nameColVector.setVal(row, element.getName().getBytes(StandardCharsets.UTF_8));
         * 		ageColVector.vector[row] = element.getAge();
         *    }
         * }
         *
         * 在程序中使用 ORC 的 Bulk-encoded Format，需要添加如下依赖：
         * <dependency>
         *     <groupId>org.apache.flink</groupId>
         *     <artifactId>flink-orc_2.12</artifactId>
         *     <version>1.15.2</version>
         * </dependency>
         *
         *
         * 类似这样使用 FileSink 以 ORC Format 输出数据：
         * String schema = "struct<_col0:string,_col1:int>";
         * DataStream<Person> input = ...;
         *
         * final OrcBulkWriterFactory<Person> writerFactory = new OrcBulkWriterFactory<>(new PersonVectorizer(schema));
         *
         * final FileSink<Person> sink = FileSink
         * 	.forBulkFormat(outputBasePath, writerFactory)
         * 	.build();
         *
         * input.sinkTo(sink);
         *
         *
         * OrcBulkWriterFactory 还可以采用 Hadoop 的 Configuration 和 Properties，这样就可以提供自定义的 Hadoop 配置 和 ORC 输出属性：
         * String schema = ...;
         * Configuration conf = ...;
         * Properties writerProperties = new Properties();
         *
         * writerProperties.setProperty("orc.compress", "LZ4");
         * // 其他 ORC 属性也可以使用类似方式进行设置
         *
         * final OrcBulkWriterFactory<Person> writerFactory = new OrcBulkWriterFactory<>(
         *     new PersonVectorizer(schema), writerProperties, conf);
         *
         *
         * 用户在重写 vectorize(...) 方法时可以调用 addUserMetadata(...) 方法来添加自己的元数据到 ORC 文件中：
         * public class PersonVectorizer extends Vectorizer<Person> implements Serializable {
         *        @Override
         *    public void vectorize(Person element, VectorizedRowBatch batch) throws IOException {
         * 		...
         * 		String metadataKey = ...;
         * 		ByteBuffer metadataValue = ...;
         * 		this.addUserMetadata(metadataKey, metadataValue);
         *    }
         * }
         *
         *
         * Hadoop SequenceFile Format：
         * 如果在程序中使用 SequenceFile 的 Bulk-encoded Format，需要添加如下依赖：
         * <dependency>
         *     <groupId>org.apache.flink</groupId>
         *     <artifactId>flink-sequence-file</artifactId>
         *     <version>1.15.2</version>
         * </dependency>
         *
         * 例如 创建一个简单的 SequenceFile：
         * import org.apache.flink.connector.file.sink.FileSink;
         * import org.apache.flink.configuration.GlobalConfiguration;
         * import org.apache.hadoop.conf.Configuration;
         * import org.apache.hadoop.io.LongWritable;
         * import org.apache.hadoop.io.SequenceFile;
         * import org.apache.hadoop.io.Text;
         *
         *
         * DataStream<Tuple2<LongWritable, Text>> input = ...;
         * Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
         * final FileSink<Tuple2<LongWritable, Text>> sink = FileSink
         *   .forBulkFormat(
         *     outputBasePath,
         *     new SequenceFileWriterFactory<>(hadoopConf, LongWritable.class, Text.class))
         * 	.build();
         *
         * input.sinkTo(sink);
         *
         * SequenceFileWriterFactory 提供额外的构造参数设置是否开启压缩功能。
         *
         * 桶分配
         *
         * 桶的逻辑定义了如何将数据分配到基本输出目录内的子目录中。
         * Row-encoded Format 和 Bulk-encoded Format 使用了DateTimeBucketAssigner作为默认分配器。
         *
         * 默认的DateTimeBucketAssigner会基于使用格式为yyyy-MM-dd--HH的系统默认时区来创建小时桶。
         * 日期格式（即桶的大小）和时区都可以手动配置。
         *
         * 还可以在格式化构造器中通过调用.withBucketAssigner(assigner)方法指定自定义的BucketAssigner。
         *
         * Flink内置了两种BucketAssigner：
         *  1. DateTimeBucketAssigner：默认的基于时间的分配器
         *  2. BasePathBucketAssigner：分配所有文件存储在基础路径上（单个全局桶）
         *
         *
         *
         *
         * 滚动策略
         *
         * RollingPolicy 定义了何时关闭给定的In-Progress Part文件，并将其转换为Pending状态，然后在转换成Finish状态。
         * Finished状态的文件，可以供查看并且可以保证数据的有效性，在出现故障时不会恢复。
         *
         * 在Streaming模式下，滚动策略（使用OnCheckpointRollingPolicy）结合Checkpoint间隔（开启Checkpoint）（到下一个Checkpoint成功时，文件的Pending状态才会转换为Finished状态）
         * 共同控制Part文件对下游readers是否可见以及这些文件的大小和数量。
         *
         * 在batch模式下，Part文件在job最后对下游才变得可见，滚动策略只控制最大的part文件大小。
         *
         * Flink 内置了两种RollingPolicies：
         * 1. DefaultRollingPolicy
         * 2. OnCheckpointRollingPolicy
         *
         *
         *
         * Part 文件生命周期
         *
         * 为了在下游使用FileSink作为输出，需要了解生成的输出文件的命名和生命周期
         *
         * Part文件可以处于以下三种状态中的任意一种：
         * 1. In-Prggress：当前正在写入的Part文件处于in-progress状态
         * 2. Pending：由于指定的滚动策略，关闭in-progress状态文件，并且等待提交
         * 3. Finished：流模式（Streaming）下的成功的Checkpoint或批模式（batch）下输入结束，文件的Pending状态转换为Finished状态
         *
         * 只有Finished状态下的文件才能被下游安全读取，并且保证不会被修改。
         *
         * 对于每个活动的桶，在任何给定时间写入Subtask中都有一个In-Progress状态的part文件，但可能有多个Pending状态和Finished状态的文件
         *
         * Part文件配置
         * Finished状态和In-Progress状态的文件只能通过命名来区分。
         *
         * 默认的，文件命名策略如下：
         * In-Progress / Pending : part-<uid>-<partFileIndex>.inprogress.uid
         * Finished : part-<uid>-<partFileIndex> 当Sink Subtask实例化时，这的uid是一个分配给Subtask的随机ID值。
         * 这个UID不具有容错机制，所以当Subtask从故障恢复时，uid会重新生成。
         *
         * Flink允许用户给part文件ming添加一个前缀和或后缀。
         * 可以使用OutputFileConfig来完成上述功能。
         *
         *
         * OutputFileConfig config = OutputFileConfig
         *  .builder()
         *  .withPartPrefix("prefix")
         *  .withPartSuffix(".ext")
         *  .build();
         *
         * FileSink<Tuple2<Integer, Integer>> sink = FileSink
         *  .forRowFormat((new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
         *  .withBucketAssigner(new KeyBucketAssigner())
         *  .withRollingPolicy(OnCheckpointRollingPolicy.build())
         *  .withOutputFileConfig(config)
         *  .build();
         *
         *
         *
         *
         * 文件合并
         *
         * 从 1.15 版本开始 FileSink 开始支持已经提交 pending 文件的合并，从而允许应用设置一个较小的时间周期并且避免生成大量的小文件。
         * 尤其是当用户使用 bulk 格式 的时候： 这种格式要求用户必须在 checkpoint 的时候切换文件。
         * 例如：
         * FileSink<Integer> fileSink=
         *         FileSink.forRowFormat(new Path(path),new SimpleStringEncoder<Integer>())
         *             .enableCompact(
         *                 FileCompactStrategy.Builder.newBuilder()
         *                     .setNumCompactThreads(1024)
         *                     .enableCompactionOnCheckpoint(5)
         *                     .build(),
         *                 new RecordWiseFileCompactor<>(
         *                     new DecoderBasedReader.Factory<>(SimpleStringDecoder::new)))
         *             .build();
         *  这一功能开启后，在文件转为 pending 状态与文件最终提交之间会进行文件合并。
         *  这些 pending 状态的文件将首先被提交为一个以 . 开头的 临时文件。
         *  这些文件随后将会按照用户指定的策略和合并方式进行合并并生成合并后的 pending 状态的文件。
         *  然后这些文件将被发送给 Committer 并提交为正式文件，在这之后，原始的临时文件也会被删除掉。
         *
         * 注意：
         * 1.一旦启用了文件合并功能，此后若需要再关闭，必须在构建FileSink时显式调用disableCompact方法
         * 2.如果启用了文件合并功能，文件可见的时间会被延长。
         * 3.如果开启了文件合并功能，sink需要设置uid，
         *   否则报异常：java.lang.IllegalStateException:
         *   Sink Sink requires to set a uid since its customized topology has set uid for some operators
         *
         *
         * 通用注意事项：
         * 1. 当使用的 Hadoop 版本 < 2.7 时， 当每次 Checkpoint 时请使用 OnCheckpointRollingPolicy 滚动 Part 文件。
         * 原因是：如果 Part 文件 “穿越” 了 Checkpoint 的时间间隔， 然后，从失败中恢复过来时，
         * FileSink 可能会使用文件系统的 truncate() 方法丢弃处于 In-progress 状态文件中的未提交数据。
         * 这个方法在 Hadoop 2.7 版本之前是不支持的，Flink 将抛出异常。
         *
         * 2. 鉴于 Flink 的 Sink 和 UDF 通常不会区分正常作业终止（例如 有限输入流）和 由于故障而终止，
         * 在 Job 正常终止时，最后一个 In-progress 状态文件不会转换为 “Finished” 状态。
         *
         * 3. Flink 和 FileSink 永远不会覆盖已提交数据。 鉴于此，假定一个 In-progress 状态文件被后续成功的 Checkpoint 提交了，
         * 当尝试从这个旧的 Checkpoint / Savepoint 进行恢复时，FileSink 将拒绝继续执行并将抛出异常，因为程序无法找到 In-progress 状态的文件。
         *
         * 4. 目前，FileSink 仅支持以下3种文件系统：HDFS、 S3 和 Local。如果在运行时使用了不支持的文件系统，Flink 将抛出异常。
         *
          */




        /**
         * 下面例子中创建了一个简单的sink，默认的将记录分配给小时桶：
         * 还指定了滚动策略，当满足以下三个条件的任何一个时都会将in-progress状态的文件进行滚动
         *  1.包含了至少20s的数据量
         *  2.从没接收延时20s之外的新记录
         *  3.文件大小已经达到1GB（写入最后一条记录之后）
         *
         */

        OutputFileConfig config = OutputFileConfig.builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        final FileSink<SensorReading> sink = FileSink.forRowFormat(
                new Path("C:\\Users\\Baisike\\opensource\\my-flink-examples\\src\\main\\resources\\output"),
                new SimpleStringEncoder<SensorReading>("UTF-8"))
                // 指定桶分配器
//                .withBucketAssigner(new BasePathBucketAssigner<>())
                // 配置日期格式
//                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd"))
                // 配置时区
//                .withBucketAssigner(new DateTimeBucketAssigner<>(ZoneId.of("America/Chicago")))
//                .withBucketAssigner(new DateTimeBucketAssigner<>(ZoneId.of("Asia/Shanghai")))
                // 指定part文件滚动策略: Streaming模式下，必须使用OnCheckpointRollingPolicy并且开启Checkpoint，
                // 否则文件将永远停留在in-progress或pending状态，并且下游系统将不能安全读取该文件数据。
                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(Duration.ofSeconds(20))
//                                .withInactivityInterval(Duration.ofSeconds(20))
//                                .withMaxPartSize(MemorySize.ofMebiBytes(1))
//                                .build()
                        OnCheckpointRollingPolicy.build()
                )
                // 指定基于滚动策略设置的检查时间间隔
//                .withBucketCheckInterval(10000)
                // 使用OutputFileConfig给part文件添加前缀和后缀
//                .withOutputFileConfig(config)
                // 文件合并（从1.15版本开始FileSink开始支持已经提交pending文件的合并，从而允许应用设置一个较小的时间周期并且避免生成大量小文件。）
                .enableCompact(
                        FileCompactStrategy.Builder.newBuilder()
                            .setNumCompactThreads(1024)
                            .enableCompactionOnCheckpoint(5)
                            .build(), new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(SimpleStringDecoder::new)))
//                .disableCompact()
                .build();

        // 文件合并的时候，sink需要设置uid，
        // 否则报异常：java.lang.IllegalStateException: Sink Sink requires to set a uid since its customized topology has set uid for some operators
        streamSource.sinkTo(sink).uid("sink");
        env.execute("file source");

    }
}
