# BigDataHadoop
# Link to the Dataset:
https://www.kaggle.com/manasgarg/ipl

# Analysis 1:
Design Patterns Used: Summarization, Sorting, Filtering.

Summarized the data based on the number of times the batsman got out by the bowler. The key and value was interchanged at the second mapper to sort the values in descending order and the top ten filter was applied to the sorted records at the reducer and the output was printed.

# Analysis 2:
Design Patterns Used: Binning

The data was binned based on the batting team and all the records was written to the respective file.

# Analysis 3:
Design Patterns Used: Partitioner

Partitioner was used to send records for a team to separate reducer. At the reducer, the data was taken based on player and the total score attained by the player for the team. The data attained was validated with the data in espncricinfo.com and was 99% accurate.

# Analysis 4:
Design Pattern Used: Bloom Filter

Filtered data based on the player name and reduced the erroneous data. 
