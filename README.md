# Similarity Join in E-commerce Transaction Logs

## Project Overview
This project aims to perform a similarity join operation on E-commerce transaction logs using Apache Spark. The dataset consists of customer purchase transaction logs collected over time, with each record containing an invoice number, a description of the item, the quantity purchased, the date of the invoice, and the unit price.

## Problem Definition
The task is to find all the similar transaction pairs across different years. The similarity between two transactions is computed using the Jaccard similarity function. A pair of transactions is considered similar if their Jaccard similarity is greater than or equal to a given threshold tau.

## Output Format
The output file contains all the similar transactions together with their similarities. The output format is "(InvoiceNo1,InvoiceNo2):similarity value". In each pair, the transactions must be from different years with InvoiceNo1 < InvoiceNo2. There should be no duplicates in the output results. The pairs are sorted in ascending order (by the first and then the second).

## Code Execution
The code should take three parameters: the input file, the output folder, and the similarity threshold tau. The command to run the code is as follows:

```bash
$ spark-submit project3.py input output tau
