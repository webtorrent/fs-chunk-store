var FSChunkStore = require('./')
var tests = require('abstract-chunk-store/tests')

tests(require('tape'), function (chunkLength) {
  return new FSChunkStore(chunkLength, { path: 'tmp/test_file' })
})
