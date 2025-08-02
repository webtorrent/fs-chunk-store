import fs from 'fs'
import FSChunkStore from './index.js'
import test from 'tape'
import { equal, text2arr } from 'uint8-util'
import parallel from 'run-parallel'
const TMP_FILE = 'tmp/test_file'

// Run abstract tests with single backing file
abstractTests(test, function (chunkLength) {
  return new FSChunkStore(chunkLength, { path: TMP_FILE })
})

// Run abstract tests with single backing file (random temp file_
abstractTests(test, function (chunkLength) {
  return new FSChunkStore(chunkLength)
})

// Run abstract tests with multiple backing files
abstractTests(test, function (chunkLength) {
  return new FSChunkStore(chunkLength, {
    files: [
      { path: 'tmp/multi1', length: 500 },
      { path: 'tmp/multi2', length: 500 }
    ]
  })
})

function makeBuffer (num) {
  const buf = new Uint8Array(10)
  buf.fill(num)
  return buf
}

// these are just 'abstract-chunk-store/tests/index.js' tests, however modified to operate on uint8's rather than buffers
function abstractTests (test, Store) {
  test('basic put, then get', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'), function (err) {
      t.error(err)
      store.get(0, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('0123456789')))
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('put invalid chunk length gives error', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123'), function (err) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.end()
      })
    })
  })

  test('concurrent puts, then concurrent gets', function (t) {
    const store = new Store(10)

    function makePutTask (i) {
      return function (cb) {
        store.put(i, makeBuffer(i), cb)
      }
    }

    function makeGetTask (i) {
      return function (cb) {
        store.get(i, function (err, data) {
          if (err) return cb(err)
          t.ok(equal(data, makeBuffer(i)))
          cb(null)
        })
      }
    }

    let tasks = []
    for (let i = 0; i < 100; i++) {
      tasks.push(makePutTask(i))
    }

    parallel(tasks, function (err) {
      t.error(err)

      tasks = []
      for (let i = 0; i < 100; i++) {
        tasks.push(makeGetTask(i))
      }

      parallel(tasks, function (err) {
        t.error(err)
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('interleaved puts and gets', function (t) {
    const store = new Store(10)
    const tasks = []

    function makeTask (i) {
      return function (cb) {
        store.put(i, makeBuffer(i), function (err) {
          if (err) return cb(err)
          store.get(i, function (err, data) {
            t.error(err)
            t.ok(equal(data, makeBuffer(i)))
            cb(null)
          })
        })
      }
    }

    for (let i = 0; i < 100; i++) {
      tasks.push(makeTask(i))
    }

    parallel(tasks, function (err) {
      t.error(err)
      store.destroy(function (err) {
        t.error(err)
        t.end()
      })
    })
  })

  test('get with `offset` and `length` options', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'), function (err) {
      t.error(err)
      store.get(0, { offset: 2, length: 3 }, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('234')))
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('get with null option', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'), function (err) {
      t.error(err)
      store.get(0, null, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('0123456789')))
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('get with empty object option', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'), function (err) {
      t.error(err)
      store.get(0, {}, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('0123456789')))
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('get with `offset` option', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'), function (err) {
      t.error(err)
      store.get(0, { offset: 2 }, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('23456789')))
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('get with `length` option', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'), function (err) {
      t.error(err)
      store.get(0, { length: 5 }, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('01234')))
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('test for sparsely populated support', function (t) {
    const store = new Store(10)
    store.put(10, Buffer.from('0123456789'), function (err) {
      t.error(err)
      store.get(10, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('0123456789')))
        store.destroy(function (err) {
          t.error(err)
          t.end()
        })
      })
    })
  })

  test('test `put` without callback - error should be silent', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('01234'))
    store.destroy(function (err) {
      t.error(err)
      t.end()
    })
  })

  test('test `put` without callback - success should be silent', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'))
    store.destroy(function (err) {
      t.error(err)
      t.end()
    })
  })

  test('chunkLength property', function (t) {
    const store = new Store(10)
    t.equal(store.chunkLength, 10)
    store.destroy(function (err) {
      t.error(err)
      t.end()
    })
  })

  test('test `get` on non-existent index', function (t) {
    const store = new Store(10)
    store.get(0, function (err, chunk) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.end()
      })
    })
  })

  test('test empty store\'s `close` calls its callback', function (t) {
    const store = new Store(10)
    store.close(function (err) {
      t.error(err)
      t.end()
    })
  })

  test('test non-empty store\'s `close` calls its callback', function (t) {
    const store = new Store(10)
    store.put(0, Buffer.from('0123456789'))
    store.close(function (err) {
      t.error(err)
      t.end()
    })
  })
}

test('length option', function (t) {
  const store = new FSChunkStore(10, { length: 20, path: TMP_FILE })
  store.put(0, Buffer.from('0123456789'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync(TMP_FILE).slice(0, 10), Buffer.from('0123456789')))
    store.put(1, Buffer.from('1234567890'), function (err) {
      t.error(err)
      t.ok(equal(fs.readFileSync(TMP_FILE), Buffer.from('01234567891234567890')))
      store.get(0, function (err, chunk) {
        t.error(err)
        t.ok(equal(chunk, text2arr('0123456789')))
        store.get(1, function (err, chunk) {
          t.error(err)
          t.ok(equal(chunk, text2arr('1234567890')))
          t.ok(equal(fs.readFileSync(TMP_FILE), Buffer.from('01234567891234567890')))
          store.destroy(function (err) {
            t.error(err)
            t.throws(function () {
              fs.readFileSync(TMP_FILE)
            })
            t.end()
          })
        })
      })
    })
  })
})

test('length option: less than chunk size', function (t) {
  const store = new FSChunkStore(10, { length: 7, path: TMP_FILE })
  store.put(0, Buffer.from('0123456'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync(TMP_FILE), Buffer.from('0123456')))
    store.get(0, function (err, chunk) {
      t.error(err)
      t.ok(equal(chunk, text2arr('0123456')))
      store.destroy(function (err) {
        t.error(err)
        t.throws(function () {
          fs.readFileSync(TMP_FILE)
        })
        t.end()
      })
    })
  })
})

test('length option: less than chunk size, write too large', function (t) {
  const store = new FSChunkStore(10, { length: 7, path: TMP_FILE })
  store.put(0, Buffer.from('0123456789'), function (err) {
    t.ok(err instanceof Error)
    store.destroy(function (err) {
      t.error(err)
      t.throws(function () {
        fs.readFileSync(TMP_FILE)
      })
      t.end()
    })
  })
})

test('length option: less than chunk size, get `offset` too large', function (t) {
  const store = new FSChunkStore(10, { length: 7, path: TMP_FILE })
  store.put(0, Buffer.from('0123456'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync(TMP_FILE), Buffer.from('0123456')))
    store.get(0, { offset: 8 }, function (err, chunk) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.throws(function () {
          fs.readFileSync(TMP_FILE)
        })
        t.end()
      })
    })
  })
})

test('length option: less than chunk size, get `length` too large', function (t) {
  const store = new FSChunkStore(10, { length: 7, path: TMP_FILE })
  store.put(0, Buffer.from('0123456'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync(TMP_FILE), Buffer.from('0123456')))
    store.get(0, { length: 8 }, function (err, chunk) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.throws(function () {
          fs.readFileSync(TMP_FILE)
        })
        t.end()
      })
    })
  })
})

test('length option: less than chunk size, get `offset + length` too large', function (t) {
  const store = new FSChunkStore(10, { length: 7, path: TMP_FILE })
  store.put(0, Buffer.from('0123456'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync(TMP_FILE), Buffer.from('0123456')))
    store.get(0, { offset: 4, length: 4 }, function (err, chunk) {
      t.ok(err instanceof Error)
      store.destroy(function (err) {
        t.error(err)
        t.throws(function () {
          fs.readFileSync(TMP_FILE)
        })
        t.end()
      })
    })
  })
})

test('multiple files', function (t) {
  const store = new FSChunkStore(10, {
    files: [
      { path: 'tmp/file1', length: 5 },
      { path: 'tmp/file2', length: 5 },
      { path: 'tmp2/file3', length: 8 },
      { path: 'tmp2/file4', length: 8 }
    ]
  })
  store.put(0, Buffer.from('0123456789'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync('tmp/file1'), Buffer.from('01234')))
    t.ok(equal(fs.readFileSync('tmp/file2'), Buffer.from('56789')))
    store.get(0, function (err, chunk) {
      t.error(err)
      t.ok(equal(chunk, text2arr('0123456789')))
      store.put(1, Buffer.from('abcdefghij'), function (err) {
        t.error(err)
        t.ok(equal(fs.readFileSync('tmp2/file3'), Buffer.from('abcdefgh')))
        store.get(1, function (err, chunk) {
          t.error(err)
          t.ok(equal(chunk, text2arr('abcdefghij')))
          store.put(2, Buffer.from('klmnop'), function (err) {
            t.error(err)
            t.ok(equal(fs.readFileSync('tmp2/file4'), Buffer.from('ijklmnop')))
            store.get(2, function (err, chunk) {
              t.error(err)
              t.ok(equal(chunk, text2arr('klmnop')))
              store.destroy(function (err) {
                t.error(err)
                t.throws(function () {
                  fs.readFileSync(TMP_FILE)
                })
                t.end()
              })
            })
          })
        })
      })
    })
  })
})

test('relative path', function (t) {
  const store = new FSChunkStore(10, {
    files: [
      { path: 'file1', length: 5 },
      { path: 'file2', length: 5 }
    ],
    path: 'tmp'
  })
  store.put(0, Buffer.from('0123456789'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync('tmp/file1'), Buffer.from('01234')))
    t.ok(equal(fs.readFileSync('tmp/file2'), Buffer.from('56789')))
    store.destroy(function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('relative path with name', function (t) {
  const store = new FSChunkStore(10, {
    files: [
      { path: 'file1', length: 5 },
      { path: 'file2', length: 5 }
    ],
    name: 'folder',
    path: 'tmp'
  })
  store.put(0, Buffer.from('0123456789'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync('tmp/file1'), Buffer.from('01234')))
    t.ok(equal(fs.readFileSync('tmp/file2'), Buffer.from('56789')))
    store.destroy(function (err) {
      t.error(err)
      t.end()
    })
  })
})

test('UID on relative path', function (t) {
  const store = new FSChunkStore(10, {
    files: [
      { path: 'file1', length: 5 },
      { path: 'file2', length: 5 }
    ],
    addUID: true,
    name: 'folder',
    path: 'tmp'
  })
  store.put(0, Buffer.from('0123456789'), function (err) {
    t.error(err)
    t.ok(equal(fs.readFileSync('tmp/folder/file1'), Buffer.from('01234')))
    t.ok(equal(fs.readFileSync('tmp/folder/file2'), Buffer.from('56789')))
    store.destroy(function (err) {
      t.error(err)
      t.end()
    })
  })
})
