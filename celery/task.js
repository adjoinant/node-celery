/* eslint-disable no-console */
const Result = require('./result');
const uuid = require('node-uuid');

const { createMessage } = require('./protocol');

module.exports = class Task {
  constructor(client, name) {
    this.client = client;
    this.name = name;
    const route = this.client.conf.ROUTES[name];
    this.queue = (route && route.queue) || this.client.conf.DEFAULT_QUEUE;
  }

  publish(args, kwargs, options = {}) {
    const id = options.id || uuid.v4();
    const { priority } = options;
    delete options.priority;
    const msg = createMessage(this.name, args, kwargs, options, id);
    const pubOptions = {
      contentType: 'application/json',
      contentEncoding: 'utf-8',
    };
    if (priority) {
      pubOptions.priority = priority;
    }

    this.client.broker.publish(this.queue, msg, pubOptions);

    return new Result(id, this.client);
  }

  call(args = [], kwargs = {}, options = {}) {
    if (!this.client.ready) {
      return this.client.emit('error', 'Client is not ready');
    }
    return this.publish(args, kwargs, options);
  }
};
