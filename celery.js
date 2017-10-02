/* eslint-disable no-console */
const util = require('util');
const amqp = require('amqp');
const events = require('events');
const uuid = require('node-uuid');

const { createMessage } = require('./protocol');

const debug = process.env.NODE_CELERY_DEBUG === '1' ? console.info : () => {};

function Configuration(options) {
  const self = this;

  for (const o in options) {
    if ({}.hasOwnProperty.call(options, o)) {
      self[o.replace(/^CELERY_/, '')] = options[o];
    }
  }

  self.BROKER_URL = self.BROKER_URL || 'amqp://';
  self.BROKER_OPTIONS = self.BROKER_OPTIONS || { url: self.BROKER_URL, heartbeat: 580 };
  self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || 'celery';
  self.DEFAULT_EXCHANGE = self.DEFAULT_EXCHANGE || '';
  self.DEFAULT_EXCHANGE_TYPE = self.DEFAULT_EXCHANGE_TYPE || 'direct';
  self.DEFAULT_ROUTING_KEY = self.DEFAULT_ROUTING_KEY || 'celery';
  self.RESULT_EXCHANGE = self.RESULT_EXCHANGE || 'celeryresults';
  self.TASK_RESULT_EXPIRES = self.TASK_RESULT_EXPIRES * 1000 || 86400000; // Default 1 day
  self.TASK_RESULT_DURABLE =
    undefined !== self.TASK_RESULT_DURABLE
      ? self.TASK_RESULT_DURABLE
      : true; // Set Durable true by default (Celery 3.1.7)
  self.ROUTES = self.ROUTES || {};
}

function Client(conf) {
  const self = this;

  self.conf = new Configuration(conf);

  self.ready = false;

  debug('Connecting to broker...');
  self.broker = amqp.createConnection(self.conf.BROKER_OPTIONS, {
    defaultExchangeName: self.conf.DEFAULT_EXCHANGE,
  });

  self.broker.on('ready', () => {
    debug('Broker connected...');
    self.ready = true;
    debug('Emiting connect event...');
    self.emit('connect');
  });

  self.broker.on('error', (err) => {
    self.emit('error', err);
  });

  self.broker.on('end', () => {
    self.emit('end');
  });
}

util.inherits(Client, events.EventEmitter);

Client.prototype.end = function end() {
  this.broker.disconnect();
};

Client.prototype.call = function call(
  name,
  ...rest /* [args], [kwargs], [options], [callback] */
) {
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
};

function Task(client, name) {
  const self = this;

  self.client = client;
  self.name = name;
  self.options = {};

  const route = self.client.conf.ROUTES[name];
  let queue = route && route.queue;

  self.publish = function pub(args, kwargs, options) {
    const id = options.id || uuid.v4();
    const priority = options.priority || self.options.priority;
    delete options.priority;
    queue = options.queue || self.options.queue || queue || self.client.conf.DEFAULT_QUEUE;
    const msg = createMessage(self.name, args, kwargs, options, id);
    const pubOptions = {
      contentType: 'application/json',
      contentEncoding: 'utf-8',
    };
    if (priority) {
      pubOptions.priority = priority;
    }

    self.client.broker.publish(queue, msg, pubOptions);

    return new Result(id, self.client);
  };
}

Task.prototype.call = function taskCall(args, kwargs, options) {
  const self = this;

  args = args || [];
  kwargs = kwargs || {};
  options = options || self.options || {};

  if (!self.client.ready) {
    return self.client.emit('error', 'Client is not ready');
  }
  return self.publish(args, kwargs, options);
};

function Result(taskid, client) {
  const self = this;

  events.EventEmitter.call(self);
  self.taskid = taskid;
  self.client = client;
  self.result = null;

  debug('Subscribing to result queue...');
  self.client.broker.queue(
    self.taskid.replace(/-/g, ''), {
      arguments: {
        'x-expires': self.client.conf.TASK_RESULT_EXPIRES,
      },
      durable: self.client.conf.TASK_RESULT_DURABLE,
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

util.inherits(Result, events.EventEmitter);

exports.createClient = (config, callback) => new Client(config, callback);

exports.createResult = (taskId, client) => new Result(taskId, client);
