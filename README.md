# refactored-eureka


Steps to run
```
git clone git@github.com:sr-vishnu/refactored-eureka.git
mkdir $HOME/{input,output}
cp refactored-eureka/input.txt $HOME/input
cd refactored-eureka/build_spark_app
sbt package
spark-submit --class "LogParser" --master local[4] ./target/scala-2.12/simple-project_2.12-1.0.jar $HOME/input/input.txt $HOME/output
```

