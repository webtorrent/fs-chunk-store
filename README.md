# fs-chunk-store [![travis][travis-image]][travis-url] [![npm][npm-image]][npm-url] [![downloads][downloads-image]][downloads-url]

[travis-image]: https://img.shields.io/travis/feross/fs-chunk-store.svg?style=flat
[travis-url]: https://travis-ci.org/feross/fs-chunk-store
[npm-image]: https://img.shields.io/npm/v/fs-chunk-store.svg?style=flat
[npm-url]: https://npmjs.org/package/fs-chunk-store
[downloads-image]: https://img.shields.io/npm/dm/fs-chunk-store.svg?style=flat
[downloads-url]: https://npmjs.org/package/fs-chunk-store

Filesystem (fs) chunk store that is [abstract-chunk-store](https://github.com/mafintosh/abstract-chunk-store) compliant

## Install

```
npm install fs-chunk-store
```

## Usage

``` js
var FSChunkStore = require('fs-chunk-store')
var chunks = new FSChunkStore(10, {
  path: '/tmp/my_file', // optional: path to backing file (will be created, if necessary)
  length: 100 // optional: total file length (in bytes)
})

chunks.put(0, new Buffer('0123456789'), function (err) {
  if (err) throw err

  chunks.get(0, function (err, chunk) {
    if (err) throw err
    console.log(chunk) // '0123456789' as a buffer

    chunks.close(function (err) {
      if (err) throw err
      console.log('/tmp/my_file file descriptor is closed')

      chunks.destroy(function (err) {
        if (err) throw err
        console.log('/tmp/my_file file is deleted')
      })
    })
  })
})
```

## License

MIT. Copyright (c) [Feross Aboukhadijeh](http://feross.org).
