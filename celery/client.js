/* eslint-disable no-console */
const Configuration = require('./configuration');
const Task = require('./task');

const amqp = require('amqp');
const events = require('events');

const debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : () => {};

module.exports = class Client extends events.EventEmitter {
  constructor(conf) {
    super();
    this.conf = new Configuration(conf);
    this.ready = false;

    debug('Connecting to broker...');
    this.broker = amqp.createConnection(this.conf.BROKER_OPTIONS, {
      defaultExchangeName: this.conf.DEFAULT_EXCHANGE,
    });

    const self = this;
    this.broker.on('ready', () => {
      debug('Broker connected...');
      self.ready = true;
      debug('Emiting connect event...');
      self.emit('connect');
    });

    this.broker.on('error', err => this.emit('error', err));
    this.broker.on('end', () => this.emit('end'));
  }

  end() { this.broker.disconnect(); }

  call(name, ...rest /* [args], [kwargs], [options], [callback] */) {
    let args;
    let kwargs;
    let options;
    let callback;
    rest.reverse().forEach((param) => {
      if (typeof param === 'function') {
        callback = param;
      } else if (Array.isArray(param)) {
        args = param;
      } else if (typeof param === 'object') {
        if (options) {
          kwargs = param;
        } else {
          options = param;
        }
      }
    });

    const task = new Task(this, name);
    const result = task.call(args, kwargs, options);

    if (callback && result) {
      debug('Subscribing to result...');
      result.on('ready', callback);
    }
    return result;
  }
};
