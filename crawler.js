var _ = require('lodash');
var jsdom = require('jsdom');
var async = require('async');
var gbk = require('gbk');
var fs = require('fs');

var schema = {
  "农产品名称": "table.Tab_mar > tbody > tr > td:nth-child(1) > a:nth-child(1).text_9pt000000",
  "市场名称": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(2):not(.kim-table-normalizer)",
  "规格": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(3):not(.kim-table-normalizer)",
  "均价": "table.Tab_mar > tbody > tr > td:nth-child(4) > div:nth-child(1)",
  "单位": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(5):not(.kim-table-normalizer)",
  "日期": "td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(1n + 2) > td:nth-child(6):not(.kim-table-normalizer)"
};

var keys = _.keys(schema);

// 该数据有 50000+ 页，生成 URL
var urlGenerator = function(pageNum) {
  return "http://www.abuya.com.cn/abuya/http/Pri_detail.jsp?pageNum=" + pageNum + "&type=&market_id=&area_type=1&search_name=&type_id=";
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
var transform = function (data, callback) {n
  callback(null, data.map(function(row){
    var original = row['市场名称'];
    var long = mapping[original];
    if (long) row['市场名称'] = long;
    return row;
  }));
};

var crawlHelper = function(pageNumTotal, cb) {
  async.mapLimit(_.range(1, pageNumTotal+1), 5, crawl, function(err, results) {
    if (err) return cb(err);
    cb(null, _.flatten(results));
  });
};

var crawl = function(pageNum, cb) {
  var url = urlGenerator(pageNum);

  gbk.fetch(url).to('string', function(err, str) {
    if (err) return cb(err);

    jsdom.env(str, function(errors, window) {
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

      console.log('finished page: '+pageNum);

      return cb(null, _.map(result[keys[0]], function(val, index) {
        var row = {};
        _.forEach(keys, function(key) {
          row[key] = result[key][index];
        });
        return row;
      }));
    });
  });
}

var getPageNumTotal = function(callback) {
  var pattern = /(\(1\/)([^]*?)(\)页，)/;

  gbk.fetch(urlGenerator(1)).to('string', function(err, str) {
    if (err) return callback(err);

    jsdom.env(str, function(errors, window) {
      if (errors) return callback(errors);

      var document = window.document;

      var numText = document.querySelector('td.Ntab_RPad10px > table.Tab_mar > tbody > tr:nth-child(22) > td:nth-child(1)').textContent.match(pattern)[2];

      console.log(numText);

      return callback(null, Number(numText));
    });
  });
}

async.waterfall(
  [
    getPageNumTotal,
    crawlHelper,
    transform
  ], function(err, data) {
    if (err) return console.dir(err);

    var result = {
      timeUpdated: Date.now(),
      count: data.length,
      results: data
    };   

    fs.writeFile('data.json', JSON.stringify(result, null, 2), function(err) {
      if (err) return console.dir(err);
      console.log('success');
    });
  });
