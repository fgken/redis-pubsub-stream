const Redis = require('ioredis');

class RedisPubSubStream {
  constructor(host = '127.0.0.1', port = 6379) {
    this.host = host;
    this.port = port;
    this.client = new Redis({
      host: this.host,
      port: this.port
    });
    this.subs = {};
    this.callbacks = {
      onmessage: () => {},
    };
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(() => resolve(), ms));
  }

  async runReader(streamName) {
    let lastId = '0-0';
    while (this.subs[streamName]) {
      const result = await this.xread(streamName, lastId);
      if (this.subs[streamName] && result.lastId !== lastId) {
        lastId = result.lastId;
        this.callbacks.onmessage(streamName, result.result);
      } else {
        await this.sleep(1000);
      }
    }
    delete this.subs[streamName];
  }

  onmessage(callback) {
    this.callbacks.onmessage = callback;
  }

  subscribe(streamName) {
    if (!(streamName in this.subs)) {
      this.subs[streamName] = true;
      this.runReader(streamName);
    }
  }

  async unsubscribe(streamName) {
    if (streamName in this.subs) {
      this.subs[streamName] = false;
    }
  }

  async xread(streamName, id = '0-0') {
    const result = {};
    let lastId = id;
    const ret = await this.client.sendCommand(
      new Redis.Command('XREAD', ['COUNT', '1', 'BLOCK', '60000', 'STREAMS', streamName, id])
    )
    if (ret === null) {
      return { result, lastId };
    }
    for (let stream of ret) {
      const key = stream[0].toString();
      result[key] = [];
      for (let record of stream[1]){
        const index = lastId = record[0].toString();
        const data = record[1];

        let dataset = [];
        for (let i = 0; i + 1 < data.length; i += 2) {
          const field = data[i].toString();
          const value = data[i + 1].toString();
          dataset.push({ field, value});
        }
        result[key].push({
          index: index,
          data: dataset
        });
      }
    }
    return { result, lastId };
  }

  close() {
    this.client.disconnect();
  }
}

module.exports = RedisPubSubStream;
