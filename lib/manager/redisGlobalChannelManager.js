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
  /**
   * @broken : we should also cleanup all 'user rooms sets'
   */
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
    self.redis.sadd(genUserKey(self, uid, sid), name, function(err) {
      utils.invokeCallback(cb, err);
    });
  });
};

GlobalChannelManager.prototype.leave = function(name, uid, sid, cb) {
  var self = this;
  // global channel redis set
  this.redis.srem(genKey(this, name, sid), uid, function(err) {
    if (err) return utils.invokeCallback(cb, err);
    // user redis set
    self.redis.srem(genUserKey(self, uid, sid), name, function(err) {
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
  var userKey = genUserKey(this, uid, sid);
  var cmds = [];
  // user redis set
  cmds.push(['del', userKey]);
  var self = this;
  // global channel redis set
  this.redis.smembers(userKey, function(err, list) {
    if(!!err) return utils.invokeCallback(cb, err);
    for(var i=0; i<list.length; i++) {
      cmds.push(['srem', genKey(self, list[i], sid), uid]);
    }
    execMultiCommands(self.redis, cmds, cb);
  });
};

GlobalChannelManager.prototype.getChannelsByServersRoomsOnes = function(servers, roomsId, onesId) {
  var channels = {};
  for(var sid in servers) {
    channels[sid] = [];
    for(var rid in roomsId){
      channels[sid].push(genKey(this, roomsId[rid], servers[sid]));
    }
    for(var oid in onesId){
      channels[sid].push(genUserChanKey(this, onesId[oid], servers[sid]));
    }
  }
  return channels;
};

/**
 * Get all members that subscribed to channels, without uid,
 * users are not duplicate in the same server
 *
 * @param {Array}     serversId List of server ids
 * @param {Object}    channels  List of channels for each servers ({'connector-1': [pomelo:globalchannel:channel:557ed3a4bcb50bc52b74744b:connector-server-1]})
 * @param {String}    uid       user id
 * @param {Function}  cb        callback function
 *
 * @return {Object}   users     List of members in channel sort by servers ({'connector-1': ['123456789', '12545757'], 'connector-2': ['123456789']})
 */
GlobalChannelManager.prototype.getMembersByMultiChannel = function(serversId, channels, uid, cb) {
  var cmds = [];
  var users = {};

  for(var sid in serversId) {
    cmds.push(['sunion', channels[sid]]);
  }

  execMultiCommands(this.redis, cmds, function (err, replies) {
    if (err) {
      return utils.invokeCallback(cb, err);
    }
    for(var i = 0; i < serversId.length; i++) {
      // remove user id from list
      var index = replies[i].indexOf(uid);
      if (index !== -1) {
        replies[i].splice(index, 1);
      }

      users[serversId[i]] = replies[i];
    }
    utils.invokeCallback(cb, null, users);
  });
};

var execMultiCommands = function(redis, cmds, cb) {
  if(!cmds.length) {
    utils.invokeCallback(cb);
    return;
  }
  redis.multi(cmds).exec(function(err, reply) {
    utils.invokeCallback(cb, err, reply);
  });
};

var genKey = function(self, name, sid) {
  return self.prefix + ':channel:' + name + ':' + sid;
};

var genUserKey = function(self, uid, sid) {
  return self.prefix + ':user:' + uid;
};

var genUserChanKey = function(self, uid, sid) {
  return self.prefix + ':channel:user:' + uid + ':' + sid;
};

var genCleanKey = function(self) {
  return self.prefix + '*';
};
