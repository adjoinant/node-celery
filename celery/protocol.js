const uuid = require('node-uuid');

const fields = ['task', 'id', 'args', 'kwargs', 'retries', 'eta', 'expires', 'queue',
  'taskset', 'chord', 'utc', 'callbacks', 'errbacks', 'timeouts'];


function formatDate(date) {
  return new Date(date).toISOString();
}

function createMessage(task, args, kwargs, options, id) {
  args = args || [];
  kwargs = kwargs || {};

  const message = {
    task,
    args,
    kwargs,
  };

  message.id = id || uuid.v4();
  for (const o in options) {
    if ({}.hasOwnProperty.call(options, o)) {
      if (fields.indexOf(o) === -1) {
        throw new Error(`invalid option: ${o}`);
      }
      message[o] = options[o];
    }
  }

  if (message.eta) {
    message.eta = formatDate(message.eta);
  }

  if (message.expires) {
    message.expires = formatDate(message.expires);
  }

  return JSON.stringify(message);
}

exports.createMessage = createMessage;
