# Click-prediction-project

This programm, given a json file, helps you predict if a user will click on an add or not. The json file must be in the same format as this file available here: https://drive.google.com/open?id=0B3dvASib_dtnQnd5NV9ORHdyamM

:warning: Note that any modification on the source file 

Requirements
- 

Before running the project, please make sure you have the following installed on your computer:
* Java 8 
* SBT 0.13.x (at least 0.13.16 is recommanded) 
* Scala 2.11.x (2.11.8 is recommanded)
* Spark 2.3.x (2.3.2 is recommanded)

Setting up the environnment  
-
* Download or clone the repository: https://github.com/Mehdqtx/Click-prediction-project.git.
* Open the file.
* Place yourself at the route directory inside de folder.
* Compile the project using: 
    $ sbt compile


Running the program
-
* Once you are in the root directory of the folder, run the programm followed by the absolute path (ABSOLUTE_PATH)of your json file you want to test, like so:
    $ sbt run "ABSOLUTE_PATH/file.json"
For example, if your json file is the desktop

* Press enter

CSV file generation 
-
Once the programm is finished, you can find the csv named "ouptut.csv" file including the predictions in the root directory of the folder.  


    
