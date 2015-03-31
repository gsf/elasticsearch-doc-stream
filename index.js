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

  self._url = url.parse(opt.url);
  self._scrollUrl = url.parse(opt.url);
  self._scrollId;

  self._url.path = self._url.path +
    '/_search?search_type=scan&scroll=10m&size=50';
  self._url.method = 'POST';
  self._scrollUrl.path = '/_search/scroll?scroll=10m';
  self._scrollUrl.headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
  };
  self._scrollUrl.method = 'POST';

  self._requestSent = false;
}

DocStream.prototype._scroll = function () {
  var self = this;
  http.request(self._scrollUrl, function (res) {
    var data = '';
    res.on('data', function (chunk) {data += chunk});
    res.on('end', function () {
      var result = JSON.parse(data);
      if (!result.hits.hits.length) return self.push(null);
      result.hits.hits.forEach(function (doc) {
        self.push(doc);
      });
      self._scroll();
    });
  }).on('error', function (e) {
    self.emit('error', e);
  }).end(self._scrollId);
};

DocStream.prototype._read = function () {
  var self = this;
  if (self._requestSent) return;
  http.request(self._url, function (res) {
    self._requestSent = true;
    var data = '';
    res.on('data', function (chunk) {data += chunk});
    res.on('end', function () {
      var result = JSON.parse(data);
      if (result.error) return self.emit('error', new Error(result.error));
      self._scrollId = result._scroll_id;
      self._scrollUrl.headers['Content-Length'] = Buffer.byteLength(self._scrollId);
      self._scroll();
    });
  }).on('error', function (e) {
    self.emit('error', e);
  }).end(JSON.stringify(this._search));
};

module.exports = DocStream;
