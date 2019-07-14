To solve this problem, we take the following approach
1. Clean up the document of all punctuation
2. Come up with a list of unique keywords in the entire corpus
3. For each document, computeTf
4. For each term , using unique words list and resutl document of step 3, compute Idf
5. Using result from step 3 and step 4, computer TfIdf Score
6. Using result from step 5, compute Similarity

We also save the interim results from step 1 to step 5 in files. These files are deleted at the end of step 6.

Assumptions:
1. cat and CAT are same words - our algorithm is case insensitive.
2. We have write permissions to the repository containing my_script.py (This is used for creating temporary files).

How to run
1. chmod u+x run.sh
2. ./run.sh /path/to/documents.txt