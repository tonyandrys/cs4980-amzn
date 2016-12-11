# Final Project: Amazon Review Analysis (Apache Spark)
## CS:4980 - Big Data Technologies
## Tony Andrys

The goal of this project was to answer the following questions about Amazon reviews, both in the specific category of music and over all reviews:
1. How are reviews distributed about individual products? How are products rated?
2. How are scores distributed? Do reviews tend to be positive, negative, or average?
3. Is there a correlation between the length of a review and the score it assigns a product?
4. How do other users perceive another review (judged by “helpfulness” scores assessed by other Amazon users)? Are reviews considered “more helpful” when they are positive, negative, or neutral?
5. Do early reviews of a product affect the scores of later reviews? In other words, is there evidence of confirmation bias among review authors?
Work on this project occurred in two phases. The first phase consisted of reformulating the data and answering a subset of the research questions from the Basic Plan. During this period, only reviews from the Music category were analyzed. All work was able to be performed on the class cluster and data was stored on the shared s3 bucket.
The second phase involved answering the remaining questions from the Basic plan, as well as expanding analysis to the rest of the dataset. Due to the size of the full dataset, work on the class cluster was almost impossible. As a result, the remainder of the project was completed on an individual cluster. All data is stored as gzips and are accessible at <s3://andrys-cs4980>.
