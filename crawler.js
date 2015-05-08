var _ = require('lodash');
var async = require('async');
var fs = require('fs');
var iconv = require('iconv-lite');
var request = require('request');
var jsdom = require('jsdom');

require('date-utils');

var PAGE_PATTERN = /(\(1\/)([^]*?)(\)页，)/;

var schema = {
  "农产品名称": "table.Tab_mar > tbody > tr > td:nth-child(1) > a:nth-child(1).text_9pt000000",
  "市场名称": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(2):not(.kim-table-normalizer)",
  "规格": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(3):not(.kim-table-normalizer)",
  "均价": "table.Tab_mar > tbody > tr > td:nth-child(4) > div:nth-child(1)",
  "单位": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(5):not(.kim-table-normalizer)",
  "日期": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(6):not(.kim-table-normalizer)"
};

var betterRequest = function(config, callback) {
  config.encoding = null; // 为了让 request 返回 buffer 而不是直接对 buffer 解码
  request(config, function(err, res, body) {
    if (err || res.statusCode!=200) return callback(err || new Error(res.statusCode));
    
    var isGBK = res.headers['content-type'].toUpperCase().indexOf('GBK')>-1;

    callback(null, isGBK?iconv.decode(body, 'GBK'):body.toString());
  });
}

var current = new Date(2011, 0, 1); //2008年10月29日

var today = Date.today();

var forwardCurrent = function() {
  current.setDate(current.getDate()+1);
};

var getLastDay = function(date) {
  var lastDay = date.clone();
  lastDay.setDate(lastDay.getDate()-1);
  return lastDay.toYMD();
}

var keys = _.keys(schema);

// 该数据有 50000+ 页，生成 URL
var urlGenerator = function(pageNum) {
  return "http://www.abuya.com.cn/abuya/http/Pri_detail.jsp?pageNum=" + pageNum;
};

var mapping = {
  "广州杨明国际兽药饲料..": "广州杨明国际兽药饲料城",
  "广州市荔朗农副产品综..": "广州市荔朗农副产品综合批发市场",
  "南沙区农林渔技术推广..": "南沙区农林渔技术推广站",
  "广州江南果菜批发市场..": "广州江南果菜批发市场经营管理有限公司",
  "白云山农产品综合批发..": "白云山农产品综合批发市场",
  "广州市百兴畜牧饲料有..": "广州市百兴畜牧饲料有限公司"
};

// 在 HTML 页面显示时因字符串太长而省略，此处补全
var transform = function (data, callback) {
  callback(null, data.map(function(row){
    var original = row['市场名称'];
    var long = mapping[original];
    if (long) row['市场名称'] = long;
    return row;
  }));
};

var isExist = function(str) {
  try{
    var fd = fs.openSync('./data/'+str, 'r');
    fs.closeSync(fd);
    return true;
  }catch(e) {
    return false;
  }
};

var getPageNumTotal = function(html, cb) {
  jsdom.env(html, function(errors, window) {
    if (errors) return cb(errors);

    var document = window.document;

    try{
      var numText = document.querySelector('td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(22) > td:nth-child(1)').textContent.match(PAGE_PATTERN)[2];
    } catch(e) {
      return cb(null, 0);
    }

    window.close();

    return cb(null, Number(numText));
  });
};

async.doWhilst(
  function(callbackDateFinished) {
    var currentDate = current.toYMD();

    if (isExist(currentDate)) return callbackDateFinished(null);

    console.log('doing '+currentDate);

    async.waterfall([

      // get html to parse total page number
      function(cb) {
        var config = {
          url: urlGenerator(1),
          method: 'POST',
          form: {
            begin_time: getLastDay(current),
            end_time: currentDate
          }
        };

        betterRequest(config, cb);
      }, 
      getPageNumTotal, //parse page number
      function(pageNumTotal, cb) {
        async.mapSeries(_.range(1, pageNumTotal+1), function(pageNum, callbackPageFinished) {

          console.log(pageNum+'/'+pageNumTotal);
          var config = {
            url: urlGenerator(pageNum),
            method: 'POST',
            form: {
              begin_time: getLastDay(current),
              end_time: currentDate
            }
          };

          betterRequest(config, function(err, html) {
            if (err) return callbackPageFinished(err);


            jsdom.env(html, function(errors, window) {
              if (errors) return cb(errors);

              var document = window.document;

              var result = {};

              _.forEach(schema, function(selector, key) {

                result[key] = _.map(document.querySelectorAll(selector), function(ele) {
                  return ele.textContent;
                });
              });

              window.close();
              if (process.memoryUsage().heapUsed > 200000000) { 
                //only call if memory use is bove 200MB
                global.gc();
              }

              return callbackPageFinished(null, _.map(result[keys[0]], function(val, index) {
                var row = {};
                _.forEach(keys, function(key) {
                  row[key] = result[key][index];
                });
                return row;
              }));
            });
          });

        }, cb);
      }, function(arr, cb) {
        var arr = _.flatten(arr);

        fs.writeFile('./data/'+current.toYMD(), 
          _.map(arr, function(item) {
            return _.map(keys, function(key) {
              return JSON.stringify(item[key]);
            });
          }).join('\n'), cb);
      }], function(err) {
        if (err) return console.log(err);
        callbackDateFinished(null);
      });
  }, 
  function() {
    forwardCurrent();
    return current.isBefore(today);
  }, function(err) {
    console.dir(err);
  });
