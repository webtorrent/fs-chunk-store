module.exports = Storage

var cuid = require('cuid')
var fs = require('fs')
var mkdirp = require('mkdirp')
var os = require('os')
var path = require('path')
var raf = require('random-access-file')
var rimraf = require('rimraf')
var thunky = require('thunky')

var TMP = fs.existsSync('/tmp') ? '/tmp' : os.tmpDir()

function Storage (chunkLength, opts) {
  var self = this
  if (!(self instanceof Storage)) return new Storage(chunkLength, opts)
  if (!opts) opts = {}

  self.chunkLength = Number(chunkLength)
  if (!self.chunkLength) throw new Error('First argument must be a chunk length')

  self.path = path.resolve(opts.path || path.join(TMP, 'fs-chunk-store', cuid()))
  self.closed = false

  if (opts.length) {
    self.lastChunkLength = (opts.length % self.chunkLength) || self.chunkLength
    self.lastChunkIndex = Math.ceil(opts.length / self.chunkLength) - 1
  }

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
  if (!cb) cb = noop
  if (self.closed) return nextTick(cb, new Error('Storage is closed'))
  self._open(function (err, file) {
    if (err) return cb(err)
    var isLastChunk = (index === self.lastChunkIndex)
    if (isLastChunk && buf.length !== self.lastChunkLength) {
      return cb(new Error('Last chunk length must be ' + self.lastChunkLength))
    }
    if (!isLastChunk && buf.length !== self.chunkLength) {
      return cb(new Error('Chunk length must be ' + self.chunkLength))
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
    var chunkLength = (index === self.lastChunkIndex)
      ? self.lastChunkLength
      : self.chunkLength
    file.read(index * self.chunkLength, chunkLength, function (err, buf) {
      if (!opts) return cb(null, buf)
      var offset = opts.offset || 0
      var len = opts.length || (buf.length - offset)
      if (offset < 0 || len <= 0 || offset + len > chunkLength) {
        return cb(new Error('Invalid offset and/or length: Max is ' + chunkLength))
      }
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

function noop () {}
