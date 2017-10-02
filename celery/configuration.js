module.exports = class Configuration {
  constructor(options) {
    for (const o in options) {
      if ({}.hasOwnProperty.call(options, o)) {
        this[o.replace(/^CELERY_/, '')] = options[o];
      }
    }

    this.BROKER_URL = this.BROKER_URL || 'amqp://';
    this.BROKER_OPTIONS = this.BROKER_OPTIONS || { url: this.BROKER_URL, heartbeat: 580 };
    this.DEFAULT_QUEUE = this.DEFAULT_QUEUE || 'celery';
    this.DEFAULT_EXCHANGE = this.DEFAULT_EXCHANGE || '';
    this.DEFAULT_EXCHANGE_TYPE = this.DEFAULT_EXCHANGE_TYPE || 'direct';
    this.DEFAULT_ROUTING_KEY = this.DEFAULT_ROUTING_KEY || 'celery';
    this.RESULT_EXCHANGE = this.RESULT_EXCHANGE || 'celeryresults';
    this.TASK_RESULT_EXPIRES = this.TASK_RESULT_EXPIRES * 1000 || 86400000; // Default 1 day
    this.TASK_RESULT_DURABLE =
      undefined !== this.TASK_RESULT_DURABLE
        ? this.TASK_RESULT_DURABLE
        : true; // Set Durable true by default (Celery 3.1.7)
    this.ROUTES = this.ROUTES || {};
  }
};
