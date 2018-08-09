var fs = require('fs');

var csv = require("fast-csv");

console.log('Hello World')

var srcFolder = '../output/target_test/filter_output/01/'
var srcFile = '01_2018-05-10_KOSPI_004565_orderbook.csv'
var tgtFolder = '../output/target_test/scaling_output/01/'
var tgtFile = '01-004565-20180510-order_test.csv'

var fast_csv = require('fast-csv');
var tempArray=new Array();

tempArray.push([
  'Code',
  'Time(etrade)',
  'Time(timestamp)',
  'SellHoga1',
  'SellHoga2',
  'SellHoga3',
  'SellHoga4',
  'SellHoga5',
  'SellHoga6',
  'SellHoga7',
  'SellHoga8',
  'SellHoga9',
  'SellHoga10',
  'SellOrder1',
  'SellOrder2',
  'SellOrder3',
  'SellOrder4',
  'SellOrder5',
  'SellOrder6',
  'SellOrder7',
  'SellOrder8',
  'SellOrder9',
  'SellOrder10',
  'BuyHoga1',
  'BuyHoga2',
  'BuyHoga3',
  'BuyHoga4',
  'BuyHoga5',
  'BuyHoga6',
  'BuyHoga7',
  'BuyHoga8',
  'BuyHoga9',
  'BuyHoga10',
  'BuyOrder1',
  'BuyOrder2',
  'BuyOrder3',
  'BuyOrder4',
  'BuyOrder5',
  'BuyOrder6',
  'BuyOrder7',
  'BuyOrder8',
  'BuyOrder9',
  'BuyOrder10',
  'TotalBuy', 'TotalSell', 'Dongsi', 'Baebun'
])

console.log("START");


let currentTime = undefined
let tempData = {}

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
    'Code': data[0],
    'Time(etrade)': data[1],
    'Time(timestamp)': data[2]
  }
  // console.log('init' + tempData['Buy executed'])
}

let pushData = (currentTime) => {
    // console.log(currentTime)
  let result = [
    tempData['Code'],
    tempData['Time(etrade)'],
    tempData['Time(timestamp)']
  ]
  
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
      tempData['Code']= data[0]
      
      // console.log('add' + tempData['Buy executed'])
      return
  } 

  pushData(currentTime)
    
  
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