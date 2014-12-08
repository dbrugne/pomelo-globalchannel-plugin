var logger = require('pomelo-logger').getLogger(__filename);

var Event = function(app) {
	this.app = app;
  this.globalChannelService = app.get('globalChannelService');
};

module.exports = Event;

Event.prototype.close_session = function(session) {
  if(!session.uid) {
    return;
  }
  // don't remove entries if another session for the same user on the same frontend remains
  var currentUserSessions = this.app.get('sessionService').getByUid(session.uid);
  if (currentUserSessions !== undefined) {
    logger.debug('at least another session exists for this user on this frontend: [%s] [%s]', session.uid, session.frontendId);
    return;
  }
  this.statusService.leaveAll(session.uid, session.frontendId, function(err) {
    if(!!err) {
      logger.error('failed to kick user from global channels on close_session: [%s] [%s], err: %j', session.uid, session.frontendId, err);
      return;
    }
  });
};