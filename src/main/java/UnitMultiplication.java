import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");
            if (fromTo.length == 1 || fromTo[1].trim().equals(""))
                return;

            outKey.set(fromTo[0]);
            String[] tos = fromTo[1].split(",");
            double prob = (double) 1 / tos.length;
            for (String to : tos){
                outVal.set(to + "=" + prob);
                context.write(outKey, outVal);
            }

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();

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
            double betaPr = Double.parseDouble(pr[1]) * (1 - beta);
            outKey.set(pr[0]);
            outVal.set(String.valueOf(betaPr));
            context.write(outKey, outVal);

        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            double prBeta = 0;
            String strVal;
            double subPr;
            for (Text val: values){
                strVal = val.toString();
                if (!strVal.contains("=")){
                    prBeta = Double.parseDouble(strVal);
                    break;
                }
            }

            for (Text val: values){
                strVal = val.toString();
                if (strVal.contains("=")){
                    String[] toProb = strVal.split("=");
                    subPr = Double.parseDouble(toProb[1]) * prBeta;
                    outKey.set(toProb[0]);
                    outVal.set(String.valueOf(subPr));
                    context.write(outKey, outVal);
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
