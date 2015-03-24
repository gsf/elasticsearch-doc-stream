# elasticsearch-doc-stream

A read stream of elasticsearch documents. Handy for reporting.

```js
var docStream = require('elasticsearch-doc-stream');

var users = [];

var d = docStream({
  url: 'http://localhost:9200/test',
  search: {
    query: {
      match: {
        title: "elasticsearch"
      }
    }
  }
});

d.on('data', function (doc) {
  var user = doc._source.user;
  if (users.indexOf(user) === -1) {
    users.push(user);
  }
});

d.on('error', function (err) {
  console.error(err);
});

d.on('end', function () {
  console.log(users);
  process.exit();
});
```
