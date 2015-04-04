var http = require('http');
var stream = require('stream');
var url = require('url');
var util = require('util');

util.inherits(DocStream, stream.Readable);

function DocStream (opt) {
  if (!(this instanceof DocStream)) {
    return new DocStream(opt);
  }
  var self = this;

  opt.objectMode = true;
  stream.Readable.call(self, opt);

  self._search = opt.search || {query: {match_all: {}}};

  self._initOpt = url.parse(opt.url);
  self._scrollOpt = url.parse(opt.url);
  self._scrollId;

  self._initOpt.path = self._initOpt.path +
    '/_search?search_type=scan&scroll=1m&size=50';
  self._initOpt.method = 'POST';
  self._scrollOpt.path = '/_search/scroll?scroll=1m';
  self._scrollOpt.headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
  };
  self._scrollOpt.method = 'POST';

  self._requestSent = false;
  self._stopped = false;
}

DocStream.prototype._scroll = function () {
  var self = this;
  http.request(self._scrollOpt, function (res) {
    var data = '';
    res.on('data', function (chunk) {data += chunk});
    res.on('end', function () {
      var result = JSON.parse(data);
      if (!result.hits.hits.length) return self.push(null);
      var pushMore = true;
      result.hits.hits.forEach(function (doc) {
        pushMore = self.push(doc);
      });
      if (pushMore) return self._scroll();
      self._stopped = true;
    });
  }).on('error', function (e) {
    self.emit('error', e);
  }).end(self._scrollId);
};

DocStream.prototype._read = function () {
  var self = this;
  if (self._stopped) {
    self._stopped = false;
    return self.scroll();
  }
  if (self._requestSent) return;
  http.request(self._initOpt, function (res) {
    self._requestSent = true;
    var data = '';
    res.on('data', function (chunk) {data += chunk});
    res.on('end', function () {
      var result = JSON.parse(data);
      if (result.error) return self.emit('error', new Error(result.error));
      self._scrollId = result._scroll_id;
      self._scrollOpt.headers['Content-Length'] = Buffer.byteLength(self._scrollId);
      self._scroll();
    });
  }).on('error', function (e) {
    self.emit('error', e);
  }).end(JSON.stringify(this._search));
};

module.exports = DocStream;
