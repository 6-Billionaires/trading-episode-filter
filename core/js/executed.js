var fs = require('fs');

var csv = require("fast-csv");

console.log('Hello World')

var srcFolder = '../../output/target/filter_output/01/'
var srcFile = '01_2018-05-10_KOSPI_004565_executed.csv'
var tgtFolder = '../../output/target/scaling_output/01/'
var tgtFile = '01-004565-20180510-quote_test.csv'


var stream = fs.createReadStream(srcFolder + srcFile);



var ws = fs.createWriteStream(tgtFolder+tgtFile); 

var fast_csv = require('fast-csv');
var tempArray=new Array();
console.log("START");
fast_csv.fromPath(srcFolder + srcFile).on("data", function(data){
    tempArray.push([data]);  
})
  .on("end", function(){
    tempArray.sort();
    console.log(tempArray);

    fast_csv.writeToPath(tgtFolder+tgtFile, tempArray)
   .on("finish", function(){
      console.log("END");
   });

}); 