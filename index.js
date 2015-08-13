module.exports = Storage

var mkdirp = require('mkdirp')
var path = require('path')
var raf = require('random-access-file')
var rimraf = require('rimraf')
var thunky = require('thunky')

function Storage (chunkLength, opts) {
  var self = this
  if (!(self instanceof Storage)) return new Storage(chunkLength, opts)

  self.path = path.resolve(opts.path)
  self.chunkLength = Number(chunkLength)
  self.closed = false

  if (!self.path) throw new Error('First argument must be a file path')
  if (!self.chunkLength) throw new Error('Second argument must be a chunk length')

  self._open = thunky(function (cb) {
    if (self.closed) return cb(new Error('Storage is closed'))
    mkdirp(path.dirname(self.path), function (err) {
      if (err) return cb(err)
      if (self.closed) return cb(new Error('Storage is closed'))

      var file = raf(self.path)
      cb(null, file)
    })
  })
}

Storage.prototype.put = function (index, buf, cb) {
  var self = this
  if (self.closed) return nextTick(cb, new Error('Storage is closed'))
  self._open(function (err, file) {
    if (err) return cb(err)
    if (buf.length !== self.chunkLength) {
      if (cb) cb(new Error('Chunk length must be ' + self.chunkLength))
      return
    }
    file.write(index * self.chunkLength, buf, cb)
  })
}

Storage.prototype.get = function (index, opts, cb) {
  var self = this
  if (typeof opts === 'function') return self.get(index, null, opts)
  if (self.closed) return nextTick(cb, new Error('Storage is closed'))
  self._open(function (err, file) {
    if (err) return cb(err)
    file.read(index * self.chunkLength, self.chunkLength, function (err, buf) {
      if (!opts) return cb(null, buf)
      var offset = opts.offset || 0
      var len = opts.length || (buf.length - offset)
      cb(null, buf.slice(offset, len + offset))
    })
  })
}

Storage.prototype.close = function (cb) {
  var self = this
  if (self.closed) return nextTick(cb, new Error('Storage is closed'))
  self.closed = true
  self._open(function (err, file) {
    // an open error is okay because that means the file is not open
    if (err) return cb(null)
    file.close(cb)
  })
}

Storage.prototype.destroy = function (cb) {
  var self = this
  self.close(function () {
    rimraf(self.path, { maxBusyTries: 60 }, cb)
  })
}

function nextTick (cb, err, val) {
  process.nextTick(function () {
    if (cb) cb(err, val)
  })
}
