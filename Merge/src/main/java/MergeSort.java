import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * @author 19856
 * @since 2023/4/10-17:18
 */


public class MergeSort {
    public static class Partition extends Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int MaxNumber = 65223;
            int bound = MaxNumber / numPartitions + 1;
            int keynumber = key.get();
            for (int i = 0; i < numPartitions; i++) {
                if (keynumber < bound * i && keynumber >= bound * (i - 1))
                    return i - 1;
            }
            return 0;
        }
    }

    public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static IntWritable data = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }
    }

    public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable linenum = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(linenum, key);
                linenum = new IntWritable(linenum.get() + 1);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.7.7");
        System.setProperty("HADOOP_USER_NAME","root");
        /**
         * Configuration:Map/Reduce的配置类，描述Hadoop框架Map-Reduce执行工作
         */
        //新建配置类
        Configuration conf = new Configuration();
        //配置resourcemanager地址
        conf.set("yarn.resourcemanager.address","hadoop100:8032");
        //允许DataNode以主机名访问
        conf.set("dfs.client.use.datanode.hostname","true");
        //配置HDFS访问地址
        conf.set("fs.defaultFS","hdfs://hadoop101:9000/");
        //配置MapReduce提交方式为跨平台提交
        conf.set("MapReduce.app-submission.cross-platform","true");
        //设置Job提交YARN去运行
        conf.set("MapReduce.framework.name","yarn");
        //设置JAR本地路径
//        conf.set("mapred.jar","C:\\Users\\19856\\Desktop\\大数据\\HadoopDemo\\WordCount\\target\\WordCount-1.0-SNAPSHOT-jar-with-dependencies.jar");
        //取得输入参数值
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length<2){
            System.err.println("Usage:wordcount<in>[<in>...]<out>");
            System.exit(2);
        }
        //设置一个用户定义的Job名称
        Job job = Job.getInstance(conf,"word count");
        //为Job设置类名
        job.setJarByClass(MergeSort.class);
        //为Job设置Mapper类
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(Partition.class);
        //为Job设置Combiner类
//        job.setCombinerClass(IntSumReducer.class);
        //为Job设置Reducer类
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //为Job的输出数据设置Key类
//        job.setOutputKeyClass(Text.class);
        //为Job输出设置Value类
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length-1; ++i) {
            //为Job设置输入路径
            FileInputFormat.setInputPaths(job,new Path(otherArgs[i]));
        }
        //为Job设置输出路径
        System.out.println("new Path(otherArgs[otherArgs.length-1]) = " + new Path(otherArgs[otherArgs.length-1]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length-1]));
        //运行Jo
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
