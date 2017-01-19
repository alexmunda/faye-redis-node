var Engine = function(server, options) {
  this._server  = server
  this._options = options || {}

  var redis = require('redis')
  var host = this._options.host || this.DEFAULT_HOST
  var port = this._options.port || this.DEFAULT_PORT
  var db = this._options.database || this.DEFAULT_DATABASE
  var auth = this._options.password
  var gc = this._options.gc || this.DEFAULT_GC
  var socket = this._options.socket
  var client = this._options.client
  var subscriberClient = this._options.subscriberClient

  this._ns  = this._options.namespace || ''

  if (client) {
    this._redis = client
  } else {
    this._redis = socket ? redis.createClient(socket, {no_ready_check: true}) : redis.createClient(port, host, {no_ready_check: true})

    if (auth) {
      this._redis.auth(auth)
    }

    this._redis.select(db)
  }

  if (subscriberClient) {
    this._subscriber = subscriberClient
  } else {
    this._subscriber = socket ? redis.createClient(socket, {no_ready_check: true}) : redis.createClient(port, host, {no_ready_check: true})

    if (auth) {
      this._subscriber.auth(auth)
    }

    this._subscriber.select(db)
  }

  this._messageChannel = this._ns + '/notifications/messages'
  this._closeChannel   = this._ns + '/notifications/close'

  var self = this
  this._subscriber.subscribe(this._messageChannel)
  this._subscriber.subscribe(this._closeChannel)
  this._subscriber.on('message', function(topic, message) {
    if (topic === self._messageChannel) self.emptyQueue(message)
    if (topic === self._closeChannel)   self._server.trigger('close', message)
  })

  this._gc = setInterval(function() { self.gc() }, gc * 1000)
}

Engine.create = function(server, options) {
  return new Engine(server, options)
}

Engine.prototype = {
  DEFAULT_HOST:     'localhost',
  DEFAULT_PORT:     6379,
  DEFAULT_DATABASE: 0,
  DEFAULT_GC:       60,
  LOCK_TIMEOUT:     120,
  MAX_TIMEOUT_S:    86400,

  disconnect: function() {
    this._redis.end()
    this._subscriber.unsubscribe()
    this._subscriber.end()
    clearInterval(this._gc)
  },

  createClient: function(callback, context) {
    var clientId = this._server.generateId(), self = this
    this._redis.zadd(this._ns + '/clients', Date.now(), clientId, function(err, added) {
      if (err) {
        self._server.error('Error creating client: ?', err)
        callback.call(context, null)
      }

      if (added === 0) {
        self._server.error('Unable to create client as id already exists ?', clientId)
        return self.createClient(callback, context)
      }

      self._server.debug('Created new client ?', clientId)
      self.ping(clientId)
      self._server.trigger('handshake', clientId)
      callback.call(context, clientId)
    })
  },

  clientExists: function(clientId, callback, context) {
    var self = this
    var cutoff = this._getCutOffTime()

    this._redis.zscore(this._ns + '/clients', clientId, function(err, score) {
      if (err) {
        self._server.error('Error checking client exists ?: ?', clientId, err)
        return callback.call(context, false)
      }

      if(!score) return callback.call(context, false)

      callback.call(context, parseInt(score, 10) > cutoff)
    })
  },

  destroyClient: function(clientId, callback, context) {
    var self = this

    this._redis.smembers(this._ns + '/clients/' + clientId + '/channels', function(err, channels) {
      if (err) {
        self._server.error('Error destroying client: ?', err)

        if (callback) callback.call(context)

        return
      }

      var multi = self._redis.multi()

      multi.zrem(self._ns + '/clients', clientId)
      multi.zrem(self._ns + '/counts', clientId)

      channels.forEach(function(channel) {
        multi.srem(self._ns + '/clients/' + clientId + '/channels', channel)
        multi.srem(self._ns + '/channels' + channel, clientId)
      })
      multi.del(self._ns + '/clients/' + clientId + '/messages')
      multi.zrem(self._ns + '/clients', clientId)
      multi.publish(self._closeChannel, clientId)

      multi.exec(function(err, results) {
        if (err) {
          self._server.error('Error destroying client: ?', err)

          if (callback) callback.call(context)

          return
        }

        channels.forEach(function(channel, i) {
          if (results[2 * i + 1] !== 1) return
          self._server.trigger('unsubscribe', clientId, channel)
          self._server.debug('Unsubscribed client ? from channel ?', clientId, channel)
        })

        self._server.debug('Destroyed client ?', clientId)
        self._server.trigger('disconnect', clientId)

        if (callback) callback.call(context)
      })
    })
  },

  ping: function(clientId) {
    var timeout = this._server.timeout
    if (typeof timeout !== 'number') return

    var time = new Date().getTime()

    this._server.debug('Ping ?, ?', clientId, time)
    this._redis.zadd(this._ns + '/clients', time, clientId)
  },

  subscribe: function(clientId, channel, callback, context) {
    var self = this
    var multi = this._redis.multi()

    multi.sadd(this._ns + '/clients/' + clientId + '/channels', channel)
    multi.sadd(this._ns + '/channels' + channel, clientId)

    multi.exec(function(err, replies) {
      if (err) {
        if (callback) callback.call(context, err)
        return
      }

      var added = replies[0]
      if (added === 1) self._server.trigger('subscribe', clientId, channel)

      self._server.debug('Subscribed client ? to channel ?', clientId, channel)

      if (callback) callback.call(context)
    })
  },

  unsubscribe: function(clientId, channel, callback, context) {
    var self = this
    var multi = this._redis.multi()

    multi.srem(this._ns + '/clients/' + clientId + '/channels', channel)
    multi.srem(this._ns + '/channels' + channel, clientId)

    multi.exec(function(err, replies) {
      if (err) {
        if (callback) callback.call(context, err)
        return
      }

      var removed = replies[0]
      if (removed === 1) self._server.trigger('unsubscribe', clientId, channel)

      self._server.debug('Unsubscribed client ? from channel ?', clientId, channel)
      if (callback) callback.call(context)
    })
  },

  publish: function(message, channels) {
    this._server.debug('Publishing message ?', message)

    var self = this
    var jsonMessage = JSON.stringify(message)
    var keys = channels.map(function(c) { return self._ns + '/channels' + c })

    var notify = function(err, clients) {
      if (err) {
        self._server.error('Error publishing message ?', err)
        return
      }

      clients.forEach(function(clientId) {
        var queue = self._ns + '/clients/' + clientId + '/messages'

        self._server.debug('Queueing for client ?: ?', clientId, message)
        self._redis.rpush(queue, jsonMessage)
        self._redis.publish(self._messageChannel, clientId)

        self.clientExists(clientId, function(exists) {
          if (!exists) self._redis.del(queue)
        })
      })
    }

    keys.push(notify)
    this._redis.sunion.apply(this._redis, keys)

    this._server.trigger('publish', message.clientId, message.channel, message.data)
  },

  emptyQueue: function(clientId) {
    if (!this._server.hasConnection(clientId)) return

    var key = this._ns + '/clients/' + clientId + '/messages'
    var multi = this._redis.multi()
    var self  = this

    multi.lrange(key, 0, -1, function(err, jsonMessages) {
      if (err) {
        self._server.error('Error emptying queue: ?', err)
        return
      }

      if (!jsonMessages) return
      var messages = jsonMessages.map(function(json) { return JSON.parse(json) })
      self._server.deliver(clientId, messages)
    })
    multi.del(key)
    multi.exec()
  },

  gc: function() {
    var self = this

    this._withLock('gc', function(releaseLock) {
      var cutoff = self._getCutOffTime(2)

      self._redis.zrangebyscore(self._ns + '/clients', 0, cutoff, function(err, clients) {
        if (err) {
          self._server.error('Error listing gc clients message ?', err)
          return releaseLock()
        }

        var i = 0
        var n = clients.length
        if (n === 0) return releaseLock()

        clients.forEach(function(clientId) {
          self.destroyClient(clientId, function() {
            i += 1
            if (i === n) releaseLock()
          })
        })
      })
    })
  },

  _getCutOffTime: function(multiplier) {
    var timeout = this._server.timeout
    if (typeof timeout !== 'number') {
      timeout = this.MAX_TIMEOUT_S /* There is always a timeout */
    }

    return Date.now() - 1000 * (multiplier || 1.6) * timeout
  },

  _withLock: function(lockName, callback, context) {
    var lockKey = this._ns + '/locks/' + lockName
    var currentTime = new Date().getTime()
    var expiry = currentTime + this.LOCK_TIMEOUT * 1000 + 1
    var self = this

    var releaseLock = function() {
      if (new Date().getTime() < expiry) self._redis.del(lockKey)
    }

    this._redis.setnx(lockKey, expiry, function(err, set) {
      if (set === 1) return callback.call(context, releaseLock)

      self._redis.get(lockKey, function(err, timeout) {
        if (!timeout) return

        var lockTimeout = parseInt(timeout, 10)
        if (currentTime < lockTimeout) return

        self._redis.getset(lockKey, expiry, function(err, oldValue) {
          if (oldValue !== timeout) return
          callback.call(context, releaseLock)
        })
      })
    })
  }
}

module.exports = Engine
