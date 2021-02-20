/*
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

package com.coderstea.bigdata.flink;

import org.apache.commons.math3.stat.inference.GTest;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReadSpecificFilesFlinkBatch {
	private static final Logger log = LoggerFactory.getLogger(ReadSpecificFilesFlinkBatch.class);
	private static final String INPUT_FOLDER = "D:\\projects\\temp\\flink-specific\\";

	public static void main(String[] args) throws Exception {
		ParameterTool param = ParameterTool.fromArgs(args);
//		get the inputFolder path from arguments otherwise pass the default path
		String inputFolder = param.get("inputFolder", INPUT_FOLDER);
		log.info("The input folder path is {}", inputFolder);

		String date = "2021_01_01";
		log.info("the input date is {}", date);

		List<Path> foldersWithGivenDate = getFoldersWithGivenDate(date, inputFolder);
		log.info("total of {} folders found for the given date", foldersWithGivenDate.size());

		//create a text input with given path to crate input from
		TextInputFormat textInputFormat = getTextInputFormatWithGivenFolders(foldersWithGivenDate);

		ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();

		List<Integer> result = env.createInput(textInputFormat)
				//convert string to integer
				.map(Integer::valueOf)
				// sum the numbers
				.reduce(Integer::sum)
				.collect();

		log.info("The sum of all the data is {}", result);


	}

	private static TextInputFormat getTextInputFormatWithGivenFolders(List<Path> foldersWithGivenDate) {

		Path[] foldersWithGivenDateArr = foldersWithGivenDate.toArray(new Path[0]);
		TextInputFormat textInputFormat = new TextInputFormat(null);
		textInputFormat.setNestedFileEnumeration(true);
		// pass the arr to the text input
		textInputFormat.setFilePaths(foldersWithGivenDateArr);
		return textInputFormat;
	}

	private static List<Path> getFoldersWithGivenDate(String date, String inputFolder) throws IOException {
		Path inputFolderPath = new Path(inputFolder);
		FileSystem fileSystem = inputFolderPath.getFileSystem();
		FileStatus[] fileStatuses = fileSystem.listStatus(inputFolderPath);
		log.info("The input folder has {} files and folders", fileStatuses.length);

		List<Path> foldersWithGivenDate = new ArrayList<>();

		for (FileStatus fileStatus : fileStatuses) {
			// it should be a directory
			if(fileStatus.isDir()){
				// get the path of the folder
				Path folderWithGivenDate = fileStatus.getPath();
				//check if the directory name contains the date
				if (folderWithGivenDate.getName().contains(date)) {
					//add it to the list
					foldersWithGivenDate.add(folderWithGivenDate);
				}
			}
		}

		log.info("Date folders with for loop size: {}", foldersWithGivenDate.size());

		// the same thing using Stream API
		List<Path> foldersWithGivenDateWithStreamApi = Stream.of(fileStatuses)
				.filter(FileStatus::isDir)
				.map(FileStatus::getPath)
				.filter(path -> path.getName().contains(date))
				.collect(Collectors.toList());

		log.info("Date folders with Stream API size: {}", foldersWithGivenDateWithStreamApi.size());

		return foldersWithGivenDateWithStreamApi;
	}

	static void createInputData() throws Exception {

		String[] dateFolderNames = "2021_01_01,2021_01_02,2021_01_03".split(",");

		Random random = new Random();
		for (int i = 1; i < 3; i++) {
			for (String dateFolderName : dateFolderNames) {
				for (int j = 0; j < 2; j++) {
					File file = new File(INPUT_FOLDER + dateFolderName + "_0" + i + "\\");
					file.mkdirs();
					FileWriter fw = new FileWriter(file.getAbsolutePath() + "\\random-numbers-0" + j + ".txt");
					for (int k = 0; k < 10; k++) {
						fw.write("" + random.nextInt(1000) + "\n");
					}
					fw.close();
				}
			}
		}



	}
}
