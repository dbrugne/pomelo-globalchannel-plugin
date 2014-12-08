var utils = require('../util/utils');
var redis = require('redis');

var DEFAULT_PREFIX = 'POMELO:CHANNEL';

var GlobalChannelManager = function(app, opts) {
  this.app = app;
  this.opts = opts || {};
  this.prefix = opts.prefix || DEFAULT_PREFIX;
  this.host = opts.host;
  this.port = opts.port;
  this.db = opts.db || '0';
  this.redis = null;
};

module.exports = GlobalChannelManager;

GlobalChannelManager.prototype.start = function(cb) {
  this.redis = redis.createClient(this.port, this.host, this.opts);
  if (this.opts.auth_pass) {
    this.redis.auth(this.opts.auth_pass);
  }
  var self = this;
  this.redis.on("error", function (err) {
      console.error("[globalchannel-plugin][redis]" + err.stack);
  });
  this.redis.once('ready', function(err) {
    if (!!err) {
      cb(err);
    } else {
      self.redis.select(self.db, cb);
    }
  });
};

GlobalChannelManager.prototype.stop = function(force, cb) {
  if(this.redis) {
    this.redis.end();
    this.redis = null;
  }
  utils.invokeCallback(cb);
};

GlobalChannelManager.prototype.clean = function(cb) {
  var cmds = [];
  var self = this;
  this.redis.keys(genCleanKey(this), function(err, list) {
    if(!!err) {
      utils.invokeCallback(cb, err);
      return;
    }
    for(var i=0; i<list.length; i++) {
      cmds.push(['del', list[i]]);
    }
    execMultiCommands(self.redis, cmds, cb);
  });
};

GlobalChannelManager.prototype.destroyChannel = function(name, cb) {
  var servers = this.app.getServers();
  var server, cmds = [];
  for(var sid in servers) {
    server = servers[sid];
    if(this.app.isFrontend(server)) {
      cmds.push(['del', genKey(this, name, sid)]);
    }
  }
  execMultiCommands(this.redis, cmds, cb);
};

GlobalChannelManager.prototype.add = function(name, uid, sid, cb) {
  var self = this;
  // add redis set for the global channel
  this.redis.sadd(genKey(this, name, sid), uid, function(err) {
    if (err) return utils.invokeCallback(cb, err);
    // add redis set for the user to store subscribed global channels
    self.redis.sadd(genUserKey(this, uid, sid), name, function(err) {
      utils.invokeCallback(cb, err);
    });
  });
};

GlobalChannelManager.prototype.leave = function(name, uid, sid, cb) {
  // global channel redis set
  this.redis.srem(genKey(this, name, sid), uid, function(err) {
    if (err) return utils.invokeCallback(cb, err);
    // user redis set
    self.redis.srem(genUserKey(this, uid, sid), name, function(err) {
      utils.invokeCallback(cb, err);
    });
  });
};

GlobalChannelManager.prototype.getMembersBySid = function(name, sid, cb) {
  this.redis.smembers(genKey(this, name, sid), function(err, list) {
    utils.invokeCallback(cb, err, list);
  });
};

GlobalChannelManager.prototype.leaveAll = function(uid, sid, cb) {
  var cmds = [];
  var self = this;
  this.redis.smembers(genUserKey(this, uid, sid), function(err, list) {
    if(!!err) return utils.invokeCallback(cb, err);
    for(var i=0; i<list.length; i++) {
      cmds.push(['srem', list[i], uid]);
    }
    execMultiCommands(self.redis, cmds, cb);
  });
};

var execMultiCommands = function(redis, cmds, cb) {
  if(!cmds.length) {
    utils.invokeCallback(cb);
    return;
  }
  redis.multi(cmds).exec(function(err, reply) {
    utils.invokeCallback(cb, err);
  });
};

var genKey = function(self, name, sid) {
  return self.prefix + ':' + name + ':' + sid;
};

var genUserKey = function(self, uid, sid) {
  return self.prefix + ':' + uid + ':' + sid;
};

var genCleanKey = function(self) {
  return self.prefix + '*';
};
