### Objective:
Given a list of customers, their transaction history, and a segmentation period, split the customers into 4 segments: Active, Inactive, New and Undefined.
  
### Installation:
This project is built using sbt . It can be imported in an IDE as a sbt project. The language used is scala. Make sure to add scala support in the IDE when importing.
You can compile and test with sbt cli using: `sbt test`  
This exercise uses Spark to compute the different segments. All dependencies are handled by sbt and should be resolved without any problem.  
 
### Definitions: 
  - endDate: after this date, transactions are not taken into account.
  - segmentation Period (**P**): period of time ending at endDate, defined by the number of days in the period
  - Activity Segments:
    - ACTIVE: the customer have transactions before P and one or more transactions in P
    - INACTIVE: the customer have transactions before P but no transactions in P
    - NEW: the first transaction for the customer is in P
    - UNDEFINED: The customer has no transactions before the end date
    
### Input parameters:
The main method expects 4 parameters:

1. dataPath: this should be `projetctDir/data`
2. outputDir
3. endDate: yyyy-MM-dd
4. segmentationPeriod 
  
### Running the job
  - When running the job from an IDE, make sure you include the "provided" dependencies in the classpath.
  - To run using command line, start sbt shell then execute `run <param> <param> ...`
### To Do:
You need to implement the methods in `src/main/scala/co/ogury/segmentation/SegmentationJob.scala` to do the following:
  
  - load the transactions and customers from the data directory using spark.
  - compute the corresponding segment for each customer.
  - save the customerId,segment couples as a csv file.
  - example output: 
      
```
    customer-id;activity-segment
    42;active
    51;inactive
    64;undefined
    7;new
    747;active
```

  - We have provided a unit tests for 2 segmentation cases. We recommend that you implement the remaining cases in `src/test/scala/co/ogury/segmentation/SegmentationJobSpec.scala` in order to validate you work.
  - run the job with endDate = 2017-12-31 and segmentationPeriod = 365
  - commit your changes.
  - Add the whole project files and results to an archive and send it by email.
  

