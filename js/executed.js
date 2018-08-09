var fs = require('fs');

var csv = require("fast-csv");

console.log('Hello World')

var srcFolder = '../output/target_test/filter_output/01/'
var srcFile = '01_2018-05-10_KOSPI_004565_executed.csv'
var tgtFolder = '../output/target_test/scaling_output/01/'
var tgtFile = '01-004565-20180510-quote_test.csv'

var fast_csv = require('fast-csv');
var tempArray=new Array();

tempArray.push([
  'Price(last executed)', 
  'Buy executed',
  'Buy Wt Price',
  'Sell executed',
  'Sell Wt Price',
  'Total executed',
  'Total Wt Price',
  'Open',
  'High',
  'Low'
])

console.log("START");


let currentTime = undefined
let tempData = {}
let showCurrentTime = false

let toMinFixed = (n, i) => {
  if (n % 1 === 0) {
    return n.toFixed(1)
  }
  return n
}
let setInitData = (data) => {
  currentTime = parseInt(data[2])
  // currentTime = new Date(data[2] * 1000).toString().split(' ')[4]
  tempData = {
    'Price(last executed)': data[3],
    'Buy executed': data[4]==='+'?Number(data[5]):0,
    'Buy Wt Price': data[4]==='+'?Number(data[3])*Number(data[5]):0,
    'Sell executed': data[4]==='-'?Number(data[5]):0,
    'Sell Wt Price': data[4]==='-'?Number(data[3])*Number(data[5]):0,
    'Open':data[6],
    'High':data[8],
    'Low':data[10]
  }
  // console.log('init' + tempData['Buy executed'])
}

let pushData = (currentTime) => {
    // console.log(currentTime)
  let result = [
    Number(tempData['Price(last executed)']).toFixed(1), 
    tempData['Buy executed'] === 0?'':tempData['Buy executed'].toFixed(1),
    tempData['Buy executed'] === 0?'':toMinFixed(tempData['Buy Wt Price'] / tempData['Buy executed'], 1),
    tempData['Sell executed'] === 0?'':tempData['Sell executed'].toFixed(1)  ,
    tempData['Sell executed'] === 0?'':toMinFixed(tempData['Sell Wt Price'] / tempData['Sell executed'], 1),
    tempData['Buy executed'] + tempData['Sell executed'] === 0?'':(tempData['Buy executed'] + tempData['Sell executed']).toFixed(1),
    tempData['Buy executed'] + tempData['Sell executed'] === 0?'':toMinFixed((tempData['Buy Wt Price'] + tempData['Sell Wt Price']) / (tempData['Buy executed'] + tempData['Sell executed']), 1),
    Number(tempData['Open']).toFixed(1), 
    Number(tempData['High']).toFixed(1), 
    Number(tempData['Low']).toFixed(1)
  ]
  if (showCurrentTime) { result.unshift(currentTime) }
  
  tempArray.push(result)
}

let pushEmptyData = (currentTime) => {
  let result = ['','','','','','','','','','']
  if (showCurrentTime) { result.unshift(currentTime) }
  tempArray.push(result)
}

fast_csv.fromPath(srcFolder + srcFile).on("data", function(data){
  // console.log(i++ + '------------------')
  if(!currentTime) {
    // at first time
    setInitData(data)
    return
  }
  
  if(currentTime === parseInt(data[2])) {
    //new Date(data[2] * 1000).toString().split(' ')[4]) {
      tempData['Price(last executed)']= data[3]
      tempData['Buy executed'] += data[4]==='+'?Number(data[5]):0
      tempData['Buy Wt Price'] += data[4]==='+'?Number(data[3])*Number(data[5]):0
      tempData['Sell executed'] += data[4]==='-'?Number(data[5]):0
      tempData['Sell Wt Price'] += data[4]==='-'?Number(data[3])*Number(data[5]):0
      tempData['Open'] = data[6]
      tempData['High'] = data[8]
      tempData['Low'] = data[10]
      
      // console.log('add' + tempData['Buy executed'])
      return
  } 

  pushData(currentTime)
    
  while(currentTime < parseInt(data[2] - 1)) {
    pushEmptyData(++currentTime)
  }
  
  setInitData(data)

})
  .on("end", function(){
    pushData(new Date(currentTime * 1000).toString().split(' ')[4])

    // while (new Date(++currentTime * 1000).toString().split(' ')[4] <= "15:40:00") {
    //   pushEmptyData(new Date(currentTime * 1000).toString().split(' ')[4])
    // }
    

    fast_csv.writeToPath(tgtFolder + tgtFile, tempArray)
   .on("finish", function(){
      console.log("END");
   });

}); 