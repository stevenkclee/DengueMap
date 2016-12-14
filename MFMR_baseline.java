import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class MFMR_baseline {

/*----------------------IntC    ----------------------*/	
	public static HashTreeNode buildHashTree(List<ItemSet> candidateItemsets, int itemsetSize)
	{
		HashTreeNode hashTreeRoot = new HashTreeNode();
		String test = null;
		HashTreeNode parentNode = null;
		HashTreeNode currNode = null;
		for(ItemSet currItemset : candidateItemsets) {
			
				parentNode = null;
				currNode = hashTreeRoot;
				for(int i=0; i < itemsetSize; i++) {
					Integer item = currItemset.getItems().get(i);
					//Integer item  = 2;
					Map<Integer, HashTreeNode> mapAtNode = currNode.getMapAtNode();
					parentNode = currNode;
					
					if(mapAtNode.containsKey(item)) {
						currNode = mapAtNode.get(item);
					}
					else {
						currNode = new HashTreeNode();
						mapAtNode.put(item, currNode);
					}
					
					parentNode.setMapAtNode(mapAtNode);
				}
				
				currNode.setLeafNode(true);
				List<ItemSet> itemsets = currNode.getItemsets();
				itemsets.add(currItemset);
				currNode.setItemsets(itemsets);
			}
			
			
		
		
		return hashTreeRoot;
	}	
	

/*----------------------IntC 2   ----------------------*/	
	public static List<ItemSet> findItemsets(HashTreeNode hashTreeRoot, Transaction t, int startIndex)
	{
		if(hashTreeRoot.isLeafNode()) {
			return hashTreeRoot.getItemsets();
		}

		List<ItemSet> matchedItemsets = new ArrayList<ItemSet>();
		for(int i=startIndex; i < t.getItems().size(); i++) {
			Integer item = t.getItems().get(i);
			Map<Integer, HashTreeNode> mapAtNode = hashTreeRoot.getMapAtNode();

			if(!mapAtNode.containsKey(item)) {
				continue;
			}
			List<ItemSet> itemset = findItemsets(mapAtNode.get(item), t, i+1);
			matchedItemsets.addAll(itemset);
		}
		
		return matchedItemsets;
	}
	
/*----------------------IntComparator_for_top_k_sort----------------------*/	
	public static class IntComparator extends WritableComparator {

	    public IntComparator() {
	        super(IntWritable.class);
	    }

	    @Override
	    public int compare(byte[] b1, int s1, int l1,
	            byte[] b2, int s2, int l2) {

	        Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
	        Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

	        return v1.compareTo(v2) * (-1);
	    }
	}
	
/*----------------Test the candidate_set is in Transaction or not-----------------------*/	
	public static boolean Intersection_check(String str1, String str2) {
		int check = 0;
		StringTokenizer tokenizer = new StringTokenizer(str1);
		ArrayList<String> transaction = new ArrayList<String>();
		while(tokenizer.hasMoreTokens()) {
			transaction.add(tokenizer.nextToken());
		}
		tokenizer = new StringTokenizer(str2);
		ArrayList<String> transaction2 = new ArrayList<String>();
		while(tokenizer.hasMoreTokens()) {
			transaction2.add(tokenizer.nextToken());
		}
		for(String tmp:transaction2) {
			if(transaction.contains(tmp)) {
				check++;
			}
		}
		if(check == transaction2.size()) {
			return true;
		}
		return false;
	}
	
/*----------------Combination C(m,n), n=2, for 1_itemsets to 2_candidatesets -----------------------*/		
	public static void Cmn_and_write(int x, int[] cm, ArrayList<String> cn, int n, Path candidate_file, FSDataOutputStream fs) throws IOException{
		String[] top_k_item_set;
		String[] top_k_item_set2;
		int min_support = 0;
		String candidateset = new String();
	    for(int i = cm[x - 1] + 1; i <= (n - 2 + x); i++){
	      cm[x] = i;
	      if(x == (2 - 1)){
	    	  top_k_item_set = cn.get(cm[0]).split("\t");
	    	  top_k_item_set2 = cn.get(cm[1]).split("\t");
	    	  if(Integer.parseInt(top_k_item_set[1]) >= Integer.parseInt(top_k_item_set2[1])) {
	    		  min_support = Integer.parseInt(top_k_item_set2[1]);
	    	  } else {
	    		  min_support = Integer.parseInt(top_k_item_set[1]);
	    	  }
	    	  candidateset = top_k_item_set[0] + " " + top_k_item_set2[0] + "\t" + min_support + "\n";
	    	  fs.writeBytes(candidateset);
	      }else{
	    	  Cmn_and_write(x+1, cm, cn, n, candidate_file, fs);
	      }
	    }
	  }

/*----------------Join two groups of itemsets, for 1_itemsets to 2_candidatesets-----------------------*/		
	public static void Join_and_write(ArrayList<String> cn1, ArrayList<String> cn2, Path candidate_file, FSDataOutputStream fs) throws IOException{
		String[] top_k_item_set;
		String[] top_k_item_set2;
		int min_support = 0;
		String candidateset = new String();
	    for(int i = 0; i < cn1.size(); i++){
	    	for(int j = 0 ; j < cn2.size(); j++) {
	  	      if((!cn1.get(i).isEmpty()) && (!cn2.get(j).isEmpty())){
	  	    	  top_k_item_set = cn1.get(i).split("\t");
	  	    	  top_k_item_set2 = cn2.get(j).split("\t");
	  	    	  if(Integer.parseInt(top_k_item_set[1]) >= Integer.parseInt(top_k_item_set2[1])) {
	  	    		  min_support = Integer.parseInt(top_k_item_set2[1]);
	  	    	  } else {
	  	    		  min_support = Integer.parseInt(top_k_item_set[1]);
	  	    	  }
	  	    	  candidateset = top_k_item_set[0] + " " + top_k_item_set2[0] + "\t" + min_support + "\n";
	  	    	  fs.writeBytes(candidateset);
	  	      }
	    	}
	      
	    }
	  }

/*----------------Combination C(m,n), n=2, for n_itemsets to n+1_candidatesets -----------------------*/	
	public static void Cmn_and_write_2(int x, int[] cm, ArrayList<String> cn, int n, Path candidate_file, FSDataOutputStream fs, int cur_itemset_size) throws IOException{
		String[] top_k_item_set;
		String[] top_k_item_set2;
		int min_support = 0;
		int generate_itemset_size = cur_itemset_size +1;
		String candidateset = new String();

	    for(int i = cm[x - 1] + 1; i <= (n - 2 + x); i++){
	      cm[x] = i;
	      if(x == (2 - 1)){
	    	  top_k_item_set = cn.get(cm[0]).split("\t");
	    	  top_k_item_set2 = cn.get(cm[1]).split("\t");
	    	  
	    	  if(Integer.parseInt(top_k_item_set[1]) >= Integer.parseInt(top_k_item_set2[1])) {
	    		  min_support = Integer.parseInt(top_k_item_set2[1]);
	    	  } else {
	    		  min_support = Integer.parseInt(top_k_item_set[1]);
	    	  }
	    	  
	    	  Set<String> attributes = new HashSet<>();
	    	  for( int k = 0; k < top_k_item_set[0].split(" ").length; k++) {
	    		  attributes.add(top_k_item_set[0].split(" ")[k]);
	    	  }
	    	  for( int k = 0; k < top_k_item_set2[0].split(" ").length; k++) {
	    		  attributes.add(top_k_item_set2[0].split(" ")[k]);
	    	  }
	    	  if(attributes.size() == generate_itemset_size) {
	    		  
	    		  String[] items = attributes.toArray(new String[attributes.size()]);
	    		  Arrays.sort(items);
	    		  for(int k=0 ; k <items.length ;k++) {
	    			  if(k==0) {
	    				  candidateset = items[k];
	    			  }else {
	    				  candidateset = candidateset + " " + items[k];
	    			  }
	    			  
	    		  }
	    		  candidateset = candidateset + "\t" + min_support + "\n";
	    		  fs.writeBytes(candidateset);
	    	  }
	      }else{
	    	  Cmn_and_write_2(x+1, cm, cn, n, candidate_file, fs, cur_itemset_size);
	      }
	    }
	  }

/*----------------Join two groups of itemsets, for n_itemsets to n+1_candidatesets -----------------------*/	
	public static void Join_and_write_2(ArrayList<String> cn1, ArrayList<String> cn2, Path candidate_file, FSDataOutputStream fs, int cur_itemset_size) throws IOException{
		String[] top_k_item_set;
		String[] top_k_item_set2;
		int min_support = 0;
		int generate_itemset_size = cur_itemset_size +1;
		String candidateset = new String();
	    for(int i = 0; i < cn1.size(); i++){
	    	for(int j = 0 ; j < cn2.size(); j++) {
	  	      if((!cn1.get(i).isEmpty()) && (!cn2.get(j).isEmpty())){
	  	    	top_k_item_set = cn1.get(i).split("\t");
	  	    	top_k_item_set2 = cn2.get(j).split("\t");
	  	    	  if(Integer.parseInt(top_k_item_set[1]) >= Integer.parseInt(top_k_item_set2[1])) {
	  	    		  min_support = Integer.parseInt(top_k_item_set2[1]);
	  	    	  } else {
	  	    		  min_support = Integer.parseInt(top_k_item_set[1]);
	  	    	  }
	  	    	  Set<String> attributes = new HashSet<>();
		    	  for( int k = 0; k < top_k_item_set[0].split(" ").length; k++) {
		    		  attributes.add(top_k_item_set[0].split(" ")[k]);
		    	  }
		    	  for( int k = 0; k < top_k_item_set2[0].split(" ").length; k++) {
		    		  attributes.add(top_k_item_set2[0].split(" ")[k]);
		    	  }
		    	  if(attributes.size() == generate_itemset_size) {
		    		  
		    		  String[] items = attributes.toArray(new String[attributes.size()]);
		    		  Arrays.sort(items);
		    		  for(int k=0 ; k <items.length ;k++) {
		    			  if(k==0) {
		    				  candidateset = items[k];
		    			  }else {
		    				  candidateset = candidateset + " " + items[k];
		    			  }
		    			  
		    		  }
		    		  
		    		  candidateset = candidateset + "\t" + min_support + "\n";
				      
				      fs.writeBytes(candidateset);
		    	  }
	  	      }
	    	}
	      
	    }
	  }

/*----------------Get_hdfs_file_content -----------------------*/	
	public static void Get_hdfs_file_content(Path file_path, int block_no, int block_size, ArrayList<String> content) throws IOException{
		String str = null;
		int block_no_tmp = 0;
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://TocServer:9000/");
		// Read itemset_data and first filter all_data_set
		FileSystem dfs_getfile = FileSystem.get(conf);
		FSDataInputStream fs = dfs_getfile.open(file_path);
		while ((str = fs.readLine())!= null)
		{	
			content.add(str);
			for(int i = 1; i < (block_size); i ++) {
				if((str = fs.readLine())!= null) {
					if(block_no_tmp == block_no){
						content.add(str);
					}
				}
			}
			if(block_no_tmp == block_no) {
				fs.close();
				return;
			}else {
				content.clear();
			}
			block_no_tmp++;
		}
		fs.close();
		return;
	 }

/*----------------------Word_count_map_reduce----------------------*/	
	static public class Word_count_map extends Mapper<LongWritable, Text, Text, Text> 
	{
		
		private Text word = new Text();
		private Text count = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();  //##########################
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			//Word count
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				count.set(Integer.toString(1));
				try {
					context.write(word, count);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		}
	}
	
	static public class Word_count_red extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			Iterator<Text> iterator = values.iterator();
			int sum = 0;
			while (iterator.hasNext()) {
				String prevVal = iterator.next().toString();
				sum += Integer.parseInt(prevVal);
			}
			
			try {
				context.write(key, new Text(Integer.toString(sum)));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.err.println("One-item-gen-Reduce: Error while setting sum = "+sum);
				e.printStackTrace();
			}
		}
	}

	
/*-----------------Candidateset_Word_count_map_reduce----------------------*/
	static public class Candidate_count_map extends Mapper<LongWritable, Text, Text, Text> 
	{
		
		private Text word = new Text();
		private Text count = new Text();
		int candidateset_num;
		int itemsize;
		List<Integer> items = new ArrayList<Integer>();
		String candidateset;
		String test;
		String[] candidateset_items;
		StringTokenizer candidateset_item;
		
		List<ItemSet> candidateItemsets = new ArrayList<ItemSet>();
		HashTreeNode hashTreeRootNode;
		ArrayList<String> candidatesets = new ArrayList<String>();
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int candidateset_count = 0;
			candidateset_num = conf.getInt("candidateset_num", 0);
			while(candidateset_count < candidateset_num) {
				
				items = new ArrayList<Integer>();
				
				
				candidateset = conf.get("Candidateset_"+candidateset_count);
				candidateset_items = candidateset.split("\t");
				
				candidateset_item = new StringTokenizer(candidateset_items[0]);
				itemsize = candidateset_item.countTokens();
				
				while(candidateset_item.hasMoreTokens()) {
					items.add(Integer.parseInt(candidateset_item.nextToken()));
				}
				
				ItemSet candidate_itemset_tmp = new ItemSet(items, 0);
				candidateItemsets.add(new ItemSet(items, 0));
				
				candidateset_count++;
				
			}
			
			
			hashTreeRootNode = buildHashTree(candidateItemsets, itemsize);
			
			
		}
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			
			count.set(Integer.toString(1));
			//word.set(test.toString());
			//context.write(word, count);
			String test = null;
			Transaction txn = AprioriUtils.getTransaction(value.toString()); //############################
			List<ItemSet> candidateItemsetsInTxn = findItemsets(hashTreeRootNode, txn, 0);
			for(ItemSet itemset : candidateItemsetsInTxn) {
				for(int i=0; i < itemsize; i++) {
					if( i == 0){
						test = itemset.getItems().get(i).toString();
					}else{
						test = test + " " + itemset.getItems().get(i).toString();
					}
				}
				word.set(test);
				context.write(word, count);
			}
			
				
		}
	}
	
	static public class Candidate_count_red extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			Iterator<Text> iterator = values.iterator();
			int sum = 0;
			while (iterator.hasNext()) {
				String prevVal = iterator.next().toString();
				sum += Integer.parseInt(prevVal);
			}
			
			try {
				context.write(key, new Text(Integer.toString(sum)));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.err.println("One-item-gen-Reduce: Error while setting sum = "+sum);
				e.printStackTrace();
			}
		}
	}	

	
/*-----------------CandidateList_filter_map_reduce----------------------*/
	static public class CandidateList_filter_map extends Mapper<LongWritable, Text, Text, Text> 
	{
		
		private Text word = new Text();
		private Text count = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{	
			String[] candidateset;
			String line = value.toString();
			candidateset = line.split("\t");
			word.set(candidateset[0]);
			count.set(candidateset[1]);
			try {
				context.write(word, count);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	static public class CandidateList_filter_red extends Reducer<Text, Text, Text, Text>
	{	
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			Iterator<Text> iterator = values.iterator();
			
			int sup = 0;
			while (iterator.hasNext()) {
				String prevVal = iterator.next().toString();
				if(sup == 0)  {
					sup = Integer.parseInt(prevVal);
				}else if(sup > Integer.parseInt(prevVal)) {
					sup = Integer.parseInt(prevVal);
				}
			}
			try {
				context.write(key, new Text(Integer.toString(sup)));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.err.println("One-item-gen-Reduce: Error while setting sum = "+sup);
				e.printStackTrace();
			}
		}
	}

	
/*-----------------Top_K_sort_map_reduce----------------------*/
	static public class Top_K_sort_map extends Mapper<LongWritable, Text, IntWritable, Text> 
	{
		
		private IntWritable count = new IntWritable();
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{	
			String line = value.toString();
			String[] itemset = line.split("\t");
			
			count.set(Integer.parseInt(itemset[1]));
			word.set(itemset[0]);
				try {
					context.write(count, word);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		}
	}
	
	static public class Top_K_sort_red extends Reducer<IntWritable, Text, Text, IntWritable>
	{
		
		boolean check_over = false;
		boolean check_over2 = false;
		static int cur_num = 0;
		static int Top_k = 0;
		private Text word = new Text();
		private IntWritable pre_count = new IntWritable();
		private IntWritable cur_count = new IntWritable();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			Iterator<Text> iterator = values.iterator();
			Top_k = context.getConfiguration().getInt("Top_k", 0);
			while (iterator.hasNext()) {
				cur_num++;
				cur_count.set(key.get());
				if((cur_num <= Top_k) || (cur_count.get() == (pre_count.get()))) {
					String tmp = new String();
					tmp = iterator.next().toString();
					word.set(tmp);
					try {
						context.write(word, cur_count);
						pre_count.set(cur_count.get());
						if(cur_num >= Top_k) {
							if(!check_over) {
								context.getCounter("Min_sup", "").increment(pre_count.get());
								check_over = true;
							}
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						System.err.println("One-item-gen-Reduce: Error while one_itemset_sort");
						e.printStackTrace();
					}
				} else 	{
					if(!check_over) {
						context.getCounter("Min_sup", "").increment(pre_count.get());
						check_over = true;
					}
					break;
				}
				
			}
				
		}
		
	}

	
/*----------------------Filter_map_reduce----------------------*/	
	
	static public class Filter_map extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		
		int curline = 0;
		private IntWritable line_no = new IntWritable();
		private Text attritube = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{	
			Configuration conf = context.getConfiguration();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String transaction = new String();
			int filter_itemsets_size;
			String passval;
			filter_itemsets_size = conf.getInt("filter_itemsets_size", 0);
			//Word count
			while (tokenizer.hasMoreTokens()) {
				

				String prevVal = tokenizer.nextToken();
				int top_k_count = 0;
				while(top_k_count < filter_itemsets_size) {
					passval = conf.get("Top_k_"+top_k_count);
					if(passval.equals(prevVal)){
						transaction += prevVal+" ";
						break;
					}
					top_k_count++;
				}

				
			}
			attritube.set(transaction);
			line_no.set(curline);
			try {
				if(!transaction.isEmpty())
					context.write(line_no, attritube);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			curline++;
		}
	}
	
	static public class Filter_red extends Reducer<IntWritable, Text, Text, NullWritable>
	{	
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{	

			Iterator<Text> iterator = values.iterator();
			try {
				context.write(new Text(iterator.next().toString()), NullWritable.get());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.err.println("One-item-gen-Reduce: Error while setting sum = ");
				e.printStackTrace();
			}
		}
	}	
	
	

/*----------------------Main function----------------------*/		
	@SuppressWarnings("deprecation")
	public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		int Top_k = Integer.parseInt(args[1]);
		int Scan_Mc = Integer.parseInt(args[2]);
		int Cur_top_k_min_sup = 0;
		int Cur_top_k_list_size = 0;
		int candidate_file_no = 1;
		
		int candidate_block_size = 100;
		int Req_top_k_min_sup = 1108;
		
		
		int filter_block_size = 1100;
		int top_k_file_no = 1;
		boolean Check_over = false;
		Date dt;
		long start,end; // Start and end time
		ArrayList<Long> step = new ArrayList<Long>();
		
		// Delete /Output folder
				Configuration conf = new Configuration();
				conf.set("fs.default.name","hdfs://TocServer:9000/");
				FileSystem dfs = FileSystem.get(conf);
				Path src = new Path("/Output");
				dfs.delete(src,true);
		
		//Start Timer
		dt = new Date();
		start = dt.getTime();
		
		
		//   Job----WordCount---------------------------------------------------------------------
		System.err.println("Word Count Start!");
		conf = new Configuration();
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(apriori_baseline.class);
		job.setMapperClass(Word_count_map.class);
		job.setCombinerClass(Word_count_red.class);
		job.setReducerClass(Word_count_red.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/Output/output_count"));
			
		if(job.waitForCompletion(true)) {
			System.err.println("Completed Word Count Generation SUCCESSFULLY \n\n");
			System.err.println("X-itemset_Top-K Start!");
		} else {
			System.err.println(" ERROR - Completed Word Count Generation for Apriori.");
		}
		dt = new Date();
		step.add(dt.getTime());
			
			
		//   Job----Get X-itemset_Top-K-----------------------------------------------------------
		conf = new Configuration();
		conf.setInt("Top_k",Top_k);
		
		job = new Job(conf, "Get X-itemset_Top-K");
		job.setJarByClass(apriori_baseline.class);
		job.setSortComparatorClass(IntComparator.class);
		job.setMapperClass(Top_K_sort_map.class);
		job.setReducerClass(Top_K_sort_red.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/Output/output_count"));
		FileOutputFormat.setOutputPath(job, new Path("/Output/output_top_k_"+top_k_file_no));
					
		if(job.waitForCompletion(true)) {
			System.err.println("Completed X-itemset_Top-K Generation SUCCESSFULLY \n\n");
			System.err.println("delete folder Start!");
		} else {
			System.err.println(" ERROR - Completed Candidate Generation for Apriori.");
		}
		dt = new Date();
		step.add(dt.getTime());
		Cur_top_k_min_sup = Integer.parseInt((job.getCounters().findCounter("Min_sup", "").getValue() + ""));
		if(Cur_top_k_min_sup < Req_top_k_min_sup) {
			Cur_top_k_min_sup = Req_top_k_min_sup;
		}
		Cur_top_k_list_size = Integer.parseInt((job.getCounters().findCounter("Cur_num", "").getValue() + ""));
		top_k_file_no ++;
			
			
		///* delete folder
		conf = new Configuration();
		conf.set("fs.default.name","hdfs://TocServer:9000/");
		dfs = FileSystem.get(conf);
		src = new Path("/Output/output_count");
		dfs.delete(src,true);
		
		dt = new Date();
		step.add(dt.getTime());
		System.err.println("delete folder SUCCESSFULLY \n\n");
		System.err.println("Read Top_k_itemset_data and generate candidate-list Start!");
		
		

		
		// Read Top_k_itemset_data and generate candidate-list	
		String str = null;	
		String[] itemset;
		conf = new Configuration();
		FileSystem dfs_getfile = FileSystem.get(conf);
		Path top_k_file = new Path("/Output/output_top_k_"+(+top_k_file_no-1)+"/part-r-00000");
		FSDataInputStream fs = dfs_getfile.open(top_k_file);

		str = null;
		while ((str = fs.readLine())!= null) {
			Cur_top_k_list_size++;
		}
		// Get_hdfs_file_content(candidate_file, 2, candidate_block_size, Content_tmp);
		ArrayList<String> Content_1 = new ArrayList<String>();
		ArrayList<String> Content_2 = new ArrayList<String>();
		int start_block_no = 0;
		int end_block_no = 1;
		int block_number = 0;
		int cur_top_k_size_tmp = Cur_top_k_list_size;
		while(cur_top_k_size_tmp > 0 ) {
			cur_top_k_size_tmp  -= candidate_block_size;
			block_number++;
		}
		
		
		// Create candidate-list file and write by algorithm
		if(end_block_no == block_number) {
			
			conf = new Configuration();
			conf.set("fs.default.name","hdfs://TocServer:9000/");
			FileSystem dfs_putfile = FileSystem.get(new URI( "hdfs://TocServer:9000" ),conf);
			Path candidate_file = new Path("hdfs://TocServer:9000/Output/output_candidate_"+(candidate_file_no-1)+"/part-r-00000");
			FSDataOutputStream fs2 = dfs_putfile.create(candidate_file);
			candidate_file_no++;
			Get_hdfs_file_content(top_k_file, 0, candidate_block_size, Content_1);
			int[] cm = new int[Content_1.size()];
			for(int i = 0; i <= (Content_1.size() - 2); i++){
			      cm[0] = i;
			      Cmn_and_write( 1, cm, Content_1, Content_1.size(), candidate_file, fs2);
			}
				
			fs2.close();		
		} else {
			conf = new Configuration();
			conf.set("fs.default.name","hdfs://TocServer:9000/");
			FileSystem dfs_putfile = FileSystem.get(new URI( "hdfs://TocServer:9000" ),conf);
			Path candidate_file = new Path("hdfs://TocServer:9000/Output/output_candidate_"+(candidate_file_no-1)+"/part-r-00000");
			FSDataOutputStream fs2 = dfs_putfile.create(candidate_file);
			candidate_file_no++;
			while (start_block_no < (block_number-1)) {
				boolean check_group = false;
				while (end_block_no <= (block_number-1)) {
					Get_hdfs_file_content(top_k_file, start_block_no, candidate_block_size, Content_1);
					if((Content_1.size() != 0 ) && (!check_group)) {
						int[] cm = new int[Content_1.size()];
						for(int i = 0; i <= (Content_1.size() - 2); i++){
							cm[0] = i;
							Cmn_and_write( 1, cm, Content_1, Content_1.size(), candidate_file, fs2);
						}
						check_group = true;
					}
						
						
					Get_hdfs_file_content(top_k_file, end_block_no, candidate_block_size, Content_2);
							
						
					if((Content_1.size() != 0) && (Content_2.size() != 0)) {
						Join_and_write(Content_1, Content_2, candidate_file, fs2);
					}
						
					if((Content_2.size() != 0 ) && (start_block_no + 1 >= (block_number-1) )) {
						int[] cm = new int[Content_2.size()];
						for(int i = 0; i <= (Content_2.size() - 2); i++) {
							cm[0] = i;
							Cmn_and_write( 1, cm, Content_2, Content_2.size(), candidate_file, fs2);
						}
						fs2.close();
					}
					Content_1.clear();
					Content_2.clear();
					end_block_no++;
						
				}
				start_block_no++;
				end_block_no = start_block_no + 1;
			}
		}
		step.add(dt.getTime());
		System.err.println("Read Top_k_itemset_data and generate candidate-list SUCCESSFULLY \n\n");
		System.err.println("Read candidate-list file and Scan database to count the sup of candidatesets Start!");	
		//System.exit(0);	
		
		
		
		// Read candidate-list file and Scan database to count the sup of candidatesets
		Configuration conf3 = new Configuration();
		conf3.set("fs.default.name","hdfs://TocServer:9000/");
		FileSystem dfs_getfile2 = FileSystem.get(conf3);
		//Path candidate_file = new Path("hdfs://TocServer:9000/Output/output_candidate_"+(candidate_file_no-1)+"/part-r-00000");
		Path candidate_file = new Path("/Output/output_candidate_0/part-r-00000");
		FSDataInputStream fs2 = dfs_getfile2.open(candidate_file);
			
		// Get Mc candidatesets from list 
		int count;
		int round = 1;
		while ((str = fs2.readLine())!= null) {
			Configuration conf2 = new Configuration();
			count = 0;
			String[] candidateset = str.split("\t");
			if(Integer.parseInt(candidateset[1]) >= Cur_top_k_min_sup) {
				conf2.set("Candidateset_"+count, str);
				System.err.println(str);
				count++;
			}	
			while ( (count < Scan_Mc) && ((str = fs2.readLine())!= null) )
			{	
				candidateset = str.split("\t");
				if(Integer.parseInt(candidateset[1]) >= Cur_top_k_min_sup) {
					conf2.set("Candidateset_"+count, str);
					//System.err.println(str);
					count++;
				}	
			}
			// Count the sup of candidatesets	
			conf2.setInt("candidateset_num", count);
			job = new Job(conf2, "Count Candidateset");
			job.setJarByClass(apriori_baseline.class);
			job.setMapperClass(Candidate_count_map.class);
			job.setCombinerClass(Candidate_count_red.class);
			job.setReducerClass(Candidate_count_red.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			//FileInputFormat.addInputPath(job, new Path("/Output/output_filter_"+(filter_file_no - 1)));
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path("/Output/output_candidateset_tmp"));
				
			if(job.waitForCompletion(true)) {
				step.add(dt.getTime());
				System.err.println("Count the sup of candidatesets SUCCESSFULLY  Round: " + round +"\n\n");
				System.err.println("Update_Top-K_List Start!");		
			} else {
				System.err.println(" ERROR - Completed Candidateset Count Generation for Apriori.");
			}
				
			//   Job----Update_Top-K_List-----------------------------------------------------------
			conf = new Configuration();
			conf.setInt("Top_k",Top_k);
			job = new Job(conf, "Update_Top-K_List");
			job.setJarByClass(apriori_baseline.class);
			job.setSortComparatorClass(IntComparator.class);
			job.setMapperClass(Top_K_sort_map.class);
			job.setReducerClass(Top_K_sort_red.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path("/Output/output_candidateset_tmp"));
			FileInputFormat.addInputPath(job, new Path("/Output/output_top_k_"+(top_k_file_no-1)));
			FileOutputFormat.setOutputPath(job, new Path("/Output/output_top_k_"+top_k_file_no));
			
			top_k_file_no++;		
			if(job.waitForCompletion(true)) {
				step.add(dt.getTime());
				System.err.println("Update_Top-K_List SUCCESSFULLY  Round: " + round +"\n\n");
				System.err.println("delete folder Start!");	
			} else {
				System.err.println(" ERROR - Completed Update_Top-K_List Generation for Apriori.");
			}
			// Update top_k_min_sup and top_k_list_size
			Cur_top_k_min_sup = Integer.parseInt((job.getCounters().findCounter("Min_sup", "").getValue() + ""));
			if(Cur_top_k_min_sup < Req_top_k_min_sup) {
				Cur_top_k_min_sup = Req_top_k_min_sup;
			}			
			Cur_top_k_list_size = Integer.parseInt((job.getCounters().findCounter("Cur_num", "").getValue() + ""));
			System.err.println("Cur_min_sup => "+ Cur_top_k_min_sup);
				
			///* delete folder
			conf = new Configuration();
			conf.set("fs.default.name","hdfs://TocServer:9000/");
			dfs = FileSystem.get(conf);
			src = new Path("/Output/output_candidateset_tmp");
			dfs.delete(src,true);
				
			round++;
			step.add(dt.getTime());
			System.err.println("delete folder  SUCCESSFULLY "+"\n\n");
			System.err.println("Count the sup of candidatesets Start!  Round: " + round +"\n\n");	
		}
		/*	
		dt = new Date();
		end = dt.getTime();
		System.err.println("Word Count Generation Start time=>" +start);
		System.err.println("Word Count Generation End time=>" +end);
		System.err.println("Total time => "+ (end-start));
		System.exit(0);	
		*/
		
		
		int itemset_size = 2;
		while(!Check_over) {
			fs2.close();
			fs.close();
			
			

			System.err.println("Filter itemset-size itemset_size="+ itemset_size+ "  Start!");
			
			conf = new Configuration();
			conf.set("fs.default.name","hdfs://TocServer:9000/");
			dfs_getfile = FileSystem.get(conf);
			// Create Candidate_tmp_file
			FileSystem dfs_putfile = FileSystem.get(conf);
			candidate_file = new Path("/Output/output_candidate_tmp"+"/part-r-00000");
			FSDataOutputStream fs3 = dfs_putfile.create(candidate_file);
			// Read top_k_file
			top_k_file = new Path("/Output/output_top_k_"+(+top_k_file_no-1)+"/part-r-00000");
			fs = dfs_getfile.open(top_k_file);
			str = null;
			Cur_top_k_list_size = 0;
			while ((str = fs.readLine())!= null)
			{	
				itemset = str.split("\t");
				itemset = itemset[0].split(" ");
				if(itemset.length == itemset_size){
			  	  	byte[] b = (str + "\n").getBytes();
				    fs3.write(b);
				    Cur_top_k_list_size++;
				}
					
			}
			fs3.close();
			fs.close();
			
			
			step.add(dt.getTime());
			System.err.println("Filter itemset-size itemset_size="+ itemset_size+ "SUCCESSFULLY \n\n");
			System.err.println("Read Top_k_itemset_data and generate candidate-list Start!"+ "itemset_size="+ itemset_size);
			//Get_hdfs_file_content(candidate_file, 2, candidate_block_size, Content_tmp);
			Content_1 = new ArrayList<String>();
			Content_2 = new ArrayList<String>();
			start_block_no = 0;
			end_block_no = 1;
			block_number = 0;
			
			// If Cur_top_k_list_size == 0, Stop Loop
			if(Cur_top_k_list_size == 0) {
				break;
			}
			
			
			cur_top_k_size_tmp = Cur_top_k_list_size;
			while(cur_top_k_size_tmp > 0 ) {
				 cur_top_k_size_tmp  -= candidate_block_size;
				 block_number++;
			}
				
				
				
				
			conf = new Configuration();
			conf.set("fs.default.name","hdfs://TocServer:9000/");
			dfs_putfile = FileSystem.get(conf);
			Path candidate_tmp_file = new Path("/Output/output_candidate_tmp"+"/part-r-00000");
			//candidate_file = new Path("/Output/output_candidate_"+(candidate_file_no-1)+"/part-r-00000");
			candidate_file = new Path("/Output/output_candidate_tmp2"+"/part-r-00000");
			fs3 = dfs_putfile.create(candidate_file);
				
			if(end_block_no == block_number) {
					
				Get_hdfs_file_content(candidate_tmp_file, 0, candidate_block_size, Content_1);
				int[] cm = new int[Content_1.size()];
				for(int i = 0; i <= (Content_1.size() - 2); i++){
				      cm[0] = i;
				      Cmn_and_write_2( 1, cm, Content_1, Content_1.size(), candidate_file, fs3, itemset_size);
				}
					
				fs3.close();			
			} else {
				while (start_block_no < (block_number-1)) {
					boolean check_group = false;
					while (end_block_no <= (block_number-1)) {
						Get_hdfs_file_content(candidate_tmp_file, start_block_no, candidate_block_size, Content_1);
						if((Content_1.size() != 0 ) && (!check_group)) {
							int[] cm = new int[Content_1.size()];
							for(int i = 0; i <= (Content_1.size() - 2); i++){
							      cm[0] = i;
							      Cmn_and_write_2( 1, cm, Content_1, Content_1.size(), candidate_file, fs3, itemset_size);
							}
							check_group = true;
						}
							
							
						Get_hdfs_file_content(candidate_tmp_file, end_block_no, candidate_block_size, Content_2);
								
							
						if((Content_1.size() != 0) && (Content_1.size() != 0)) {
							Join_and_write_2(Content_1, Content_2, candidate_file, fs3, itemset_size);
						}
						
						if((Content_2.size() != 0 ) && (start_block_no + 1 >= (block_number-1) )) {
							int[] cm = new int[Content_2.size()];
							for(int i = 0; i <= (Content_2.size() - 2); i++){
								cm[0] = i;
								Cmn_and_write_2( 1, cm, Content_2, Content_2.size(), candidate_file, fs3, itemset_size);
							}
							fs3.close();
						}
						Content_1.clear();
						Content_2.clear();
						end_block_no++;
							
					}
					start_block_no++;
					end_block_no = start_block_no + 1;
				}
			}
			
			step.add(dt.getTime());
			System.err.println("Read Top_k_itemset_data and generate candidate-list itemset_size="+ itemset_size+ "SUCCESSFULLY \n\n");
			System.err.println("delete folder Start!");
			
			///* delete folder	
			conf = new Configuration();
			conf.set("fs.default.name","hdfs://TocServer:9000/");
			dfs = FileSystem.get(conf);
			src = new Path("/Output/output_candidate_tmp");
			dfs.delete(src,true);
			
			step.add(dt.getTime());
			System.err.println("delete folder itemset_size="+ itemset_size+ "SUCCESSFULLY \n\n");
			System.err.println("delete folder Start!");
				
			//   Job----Candidate_List_filter Count---------------------------------------------------------------------
			conf = new Configuration();
			job = new Job(conf, "Candidate_count");
			job.setJarByClass(apriori_baseline.class);
			job.setMapperClass(CandidateList_filter_map.class);
			job.setCombinerClass(CandidateList_filter_red.class);
			job.setReducerClass(CandidateList_filter_red.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path("/Output/output_candidate_tmp2"));
			FileOutputFormat.setOutputPath(job, new Path("/Output/output_candidate_"+(candidate_file_no-1)));
				
			if(job.waitForCompletion(true)) {
				System.err.println("Completed Candidate_List_filter SUCCESSFULLY for Apriori.");
				step.add(dt.getTime());
				System.err.println("delete folder itemset_size="+ itemset_size+ "SUCCESSFULLY \n\n");
				System.err.println("delete folder Start!");
			} else {
				System.err.println(" ERROR - Completed Candidate_List_filter for Apriori.");
			}
			///* delete folder	
			conf = new Configuration();
			conf.set("fs.default.name","hdfs://TocServer:9000/");
			dfs = FileSystem.get(conf);
			src = new Path("/Output/output_candidate_tmp2");
			dfs.delete(src,true);
				
			//--------------------------------------------------------------------------------------------------------------		
			conf3 = new Configuration();
			conf3.set("fs.default.name","hdfs://TocServer:9000/");
			dfs_getfile2 = FileSystem.get(conf3);
			candidate_file = new Path("hdfs://TocServer:9000/Output/output_candidate_"+(candidate_file_no-1)+"/part-r-00000");
			//candidate_file = new Path("/Output/output_candidate_0/part-r-00000");
			fs2 = dfs_getfile2.open(candidate_file);
				
			while ((str = fs2.readLine())!= null) {
					
				Configuration conf2 = new Configuration();
				count = 0;
				String[] candidateset = str.split("\t");
				if(Integer.parseInt(candidateset[1]) >= Cur_top_k_min_sup) {
					conf2.set("Candidateset_"+count, str);
					System.err.println(str);
					count++;
				}	
				while ( (count < Scan_Mc) && ((str = fs2.readLine())!= null) )
				{	
					candidateset = str.split("\t");
					if(Integer.parseInt(candidateset[1]) >= Cur_top_k_min_sup) {
						conf2.set("Candidateset_"+count, str);
						//System.err.println(str);
						count++;
					}	
				}
					
				conf2.setInt("candidateset_num", count);
					
				job = new Job(conf2, "Count Candidateset");
				job.setJarByClass(apriori_baseline.class);
				job.setMapperClass(Candidate_count_map.class);
				job.setCombinerClass(Candidate_count_red.class);
				job.setReducerClass(Candidate_count_red.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				//FileInputFormat.addInputPath(job, new Path("/Output/output_filter_"+(filter_file_no - 1)));
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path("/Output/output_candidateset_tmp"));
				
				if(job.waitForCompletion(true)) {
					System.err.println("Completed Candidateset Count Generation SUCCESSFULLY for Apriori. Round: " + round);	
				} else {
					System.err.println(" ERROR - Completed Candidate Generation for Apriori.");
				}
					
				//   Job----Update_Top-K-----------------------------------------------------------
				conf = new Configuration();
				conf.setInt("Top_k",Top_k);
				job = new Job(conf, "Update_Top-K");
				job.setJarByClass(apriori_baseline.class);
				job.setSortComparatorClass(IntComparator.class);
				job.setMapperClass(Top_K_sort_map.class);
				job.setReducerClass(Top_K_sort_red.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				FileInputFormat.addInputPath(job, new Path("/Output/output_candidateset_tmp"));
				FileInputFormat.addInputPath(job, new Path("/Output/output_top_k_"+(top_k_file_no-1)));
				FileOutputFormat.setOutputPath(job, new Path("/Output/output_top_k_"+top_k_file_no));
				top_k_file_no++;		
				if(job.waitForCompletion(true)) {
					System.err.println("Completed Update_Top-K_List SUCCESSFULLY for Apriori. Round: " + round);
				} else {
					System.err.println(" ERROR - Completed Candidate Generation for Apriori.");
				}
				
				Cur_top_k_min_sup = Integer.parseInt((job.getCounters().findCounter("Min_sup", "").getValue() + ""));
				if(Cur_top_k_min_sup < Req_top_k_min_sup) {
					Cur_top_k_min_sup = Req_top_k_min_sup;
				}				
				Cur_top_k_list_size = Integer.parseInt((job.getCounters().findCounter("Cur_num", "").getValue() + ""));
				
				System.err.println("Cur_min_sup => "+ Cur_top_k_min_sup);
				///* delete folder
				conf = new Configuration();
				conf.set("fs.default.name","hdfs://TocServer:9000/");
				dfs = FileSystem.get(conf);
				src = new Path("/Output/output_candidateset_tmp");
				dfs.delete(src,true);
//--------------------------------------------------------------------------------------------------------------	
				}
				round++;
				candidate_file_no++;
				itemset_size++;
				
		}
			
			
		//End Timer
		dt = new Date();
		end = dt.getTime();
		System.err.println("Word Count Generation Start time=>" +start);
		System.err.println("Word Count Generation End time=>" +end);

		System.err.println("Total time => "+ (end-start));
		System.err.println("Cur_min_sup => "+ Cur_top_k_min_sup);
		System.err.println("Cur_top_k_size => "+ Cur_top_k_list_size);
		System.err.println("candidate_block_size => "+ candidate_block_size);
		System.err.println("Block_number => "+ block_number);
		
		long tmpk = start;
		for(Long x:step) {
			if((x-tmpk) != 0){
				System.err.println("time => "+ (x-tmpk));
			}
			
			tmpk = x;
		}
		
	}
	
	
}

