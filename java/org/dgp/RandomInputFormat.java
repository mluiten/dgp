/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dgp;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * RandomInputFormat so we can
 */
public class RandomInputFormat extends InputFormat<Text, Text> {

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      int numSplits = 100;
      ArrayList<InputSplit> result = new ArrayList<InputSplit>(numSplits);
      Path outDir = FileOutputFormat.getOutputPath(job);
      for(int i=0; i < numSplits; ++i) {
        result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, 
                                  (String[])null));
      }
      return result;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader extends RecordReader<Text, Text> {
      Path name;

      Text key;
      Text value;

      public RandomRecordReader(Path p) {
        name = p;
        key = new Text();
        value = new Text();
      }

      public void initialize(InputSplit genericSplit,
                             TaskAttemptContext context) throws IOException {
      }

      public boolean nextKeyValue() {
        if (name != null) {
          key.set(name.getName());
          name = null;
          return true;
        }
        return false;
      }

      public Text getCurrentKey() {
        return new Text("()");
      }
      public Text getCurrentValue() {
        return new Text("()");
      }
      public void close() {}

      public float getProgress() {
        return 0.0f;
      }
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            TaskAttemptContext job) throws IOException, InterruptedException {
      return new RandomRecordReader(((FileSplit) split).getPath());
    }
}

