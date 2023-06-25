# TF-IDF Computation and Song Analysis with PySpark

This project focuses on computing the Term Frequency-Inverse Document Frequency (TF-IDF) values for each token in a lyrics dataset using PySpark. Additionally, it involves finding the word with the highest TF-IDF score for each song and identifying the top three songs that can be categorized as having a 'sad' mood based on the frequency of specific keywords.

## Prerequisites

Before running the code, ensure you have the following:

- Apache Spark installed and properly configured.
- The lyrics dataset in a compatible format (e.g., CSV or text file).

## Steps

1. Read the Lyrics Dataset: Load the lyrics dataset into a PySpark RDD or DataFrame. You can use the appropriate Spark API function to read the data based on the file format.

2. Preprocessing: Perform necessary preprocessing steps on the dataset, such as removing punctuations and converting text to lowercase. These steps ensure accurate TF-IDF calculations.

3. Calculate Term Frequency (TF): Compute the Term Frequency for each token in every song using the formula: TF = (number of occurrences of token in the song) / (total number of tokens in the song). You can utilize PySpark's built-in functions and transformations to accomplish this.

4. Calculate Inverse Document Frequency (IDF): Determine the Inverse Document Frequency for each token using the formula: IDF = log((total number of songs) / (number of songs containing the token)). Utilize Spark's aggregation and statistical functions to calculate IDF.

5. Compute TF-IDF: Multiply the TF and IDF values to obtain the TF-IDF score for each token in each song. Create a new DataFrame that includes columns for Song ID, Token, Term Frequency, Inverse Document Frequency, and TF-IDF.

6. Find Words with Highest TF-IDF: Use Spark SQL to group the data by Song ID and identify the word with the highest TF-IDF score for each song. This can be achieved by grouping the data and using the "max" function to find the maximum TF-IDF value within each group.

7. Identify Top 3 'Sad' Mood Songs: Filter the DataFrame based on specific keywords associated with a 'sad' mood, such as "tear," "feel," and "hate." Count the occurrences of these keywords in each song and rank them based on frequency. Retrieve the names of the top three songs.

## Usage

1. Prepare the environment by installing and configuring Apache Spark.

2. Ensure the lyrics dataset is available in a compatible format (e.g., CSV or text file).

3. Customize the provided code outline to fit your dataset and requirements.

4. Run the code using a PySpark-compatible environment or submit it as a Spark job.

## Conclusion

By following the outlined steps and customizing the code to your specific dataset, you can compute the TF-IDF values for each token in the lyrics dataset and extract valuable insights from the data. The project enables you to identify the words with the highest TF-IDF scores for each song and discover the top three songs that exhibit a 'sad' mood based on keyword frequency.
