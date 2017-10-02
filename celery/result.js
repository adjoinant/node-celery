/* eslint-disable no-console */
const events = require('events');

const debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : () => {};

module.exports = class Result extends events.EventEmitter {
  constructor(taskid, client) {
    super();

    this.taskid = taskid;
    this.client = client;
    this.result = null;

    debug('Subscribing to result queue...');
    const self = this;
    this.client.broker.queue(
      this.taskid.replace(/-/g, ''), {
        arguments: {
          'x-expires': this.client.conf.TASK_RESULT_EXPIRES,
        },
        durable: this.client.conf.TASK_RESULT_DURABLE,
        closeChannelOnUnsubscribe: true,
      },
      (q) => {
        q.bind(self.client.conf.RESULT_EXCHANGE, '#');
        let ctag;
        q.subscribe((message) => {
          if (message.contentType === 'application/x-python-serialize') {
            console.error('Celery should be configured with json serializer');
            process.exit(1);
          }
          self.result = message;
          q.unsubscribe(ctag);
          debug('Emiting ready event...');
          self.emit('ready', message);
          debug('Emiting task status event...');
          self.emit(message.status.toLowerCase(), message);
        }).addCallback((ok) => { ctag = ok.consumerTag; });
      }
    );
  }
};
