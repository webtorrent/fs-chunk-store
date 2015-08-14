var FSChunkStore = require('./')
var abstractTests = require('abstract-chunk-store/tests')
var test = require('tape')

var TEMP_FILE = 'tmp/test_file'

abstractTests(test, function (chunkLength) {
  return new FSChunkStore(chunkLength, { path: TEMP_FILE })
})

test('length option', function (t) {
  var store = new FSChunkStore(10, { path: TEMP_FILE, length: 20 })
  store.put(0, new Buffer('0123456789'), function (err) {
    t.error(err)
    store.put(1, new Buffer('1234567890'), function (err) {
      t.error(err)
      store.get(0, function (err, chunk) {
        t.error(err)
        t.deepEqual(chunk, new Buffer('0123456789'))
        store.get(1, function (err, chunk) {
          t.error(err)
          t.deepEqual(chunk, new Buffer('1234567890'))
          store.destroy(function (err) {
            t.error(err)
            t.end()
          })
        })
      })
    })
  })
})

test('length option: less than chunk size', function (t) {
  var store = new FSChunkStore(10, { path: TEMP_FILE, length: 7 })
  store.put(0, new Buffer('0123456'), function (err) {
    t.error(err)
    store.get(0, function (err, chunk) {
      t.error(err)
      t.deepEqual(chunk, new Buffer('0123456'))
      store.destroy(function (err) {
        t.error(err)
        t.end()
      })
    })
  })
})

test('length option: less than chunk size, write too large', function (t) {
  var store = new FSChunkStore(10, { path: TEMP_FILE, length: 7 })
  store.put(0, new Buffer('0123456789'), function (err) {
    t.ok(err instanceof Error)
    store.destroy(function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('length option: less than chunk size, get `offset` too large', function (t) {
  var store = new FSChunkStore(10, { path: TEMP_FILE, length: 7 })
  store.put(0, new Buffer('0123456'), function (err) {
    t.error(err)
    store.get(0, { offset: 8 }, function (err, chunk) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.end()
      })
    })
  })
})

test('length option: less than chunk size, get `length` too large', function (t) {
  var store = new FSChunkStore(10, { path: TEMP_FILE, length: 7 })
  store.put(0, new Buffer('0123456'), function (err) {
    t.error(err)
    store.get(0, { length: 8 }, function (err, chunk) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.end()
      })
    })
  })
})

test('length option: less than chunk size, get `offset + length` too large', function (t) {
  var store = new FSChunkStore(10, { path: TEMP_FILE, length: 7 })
  store.put(0, new Buffer('0123456'), function (err) {
    t.error(err)
    store.get(0, { offset: 4, length: 4 }, function (err, chunk) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.end()
      })
    })
  })
})
