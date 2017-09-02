import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text outKey = new Text();
        private DoubleWritable outVal = new DoubleWritable();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: toPage\t unitMultiplication
            //target: pass to reducer
            String line = value.toString().trim();
            String[] pageSubrank = line.split("\t");
            outKey.set(pageSubrank[0]);
            outVal.set(Double.parseDouble(pageSubrank[1]));
            context.write(outKey, outVal);
        }
    }

    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text outKey = new Text();
        private DoubleWritable outVal = new DoubleWritable();
        private float beta;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String line = value.toString().trim();
            String[] pr = line.split("\t");
            double prBeta = Double.parseDouble(pr[1]) * beta;
            outKey.set(pr[0]);
            outVal.set(prBeta);
            context.write(outKey, outVal);

        }
    }


    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private Text outKey = new Text();
        private DoubleWritable outVal = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

           //input key = toPage value = <unitMultiplication>
            //target: sum!

            double sum = 0;
            for (DoubleWritable subPr: values){
                sum += subPr.get();
            }
            outKey.set(key);
            outVal.set(sum);
            context.write(outKey, outVal);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

        ChainMapper.addMapper(job, PassMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        ChainMapper.addMapper(job, BetaMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
