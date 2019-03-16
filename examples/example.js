const RedisPubSubStream = require('redis-pubsub-stream');

(async () => {
  const redis = new RedisPubSubStream('172.16.0.200', 30379);

  let result = await redis.xread('test');
  console.log(JSON.stringify(result.result, null, '  '));
  result = await redis.xread('test', result.lastId);
  console.log(JSON.stringify(result.result, null, '  '));

  redis.onmessage((key, res) => {
    console.log(key, res);
  });
  redis.subscribe('test');
  setTimeout(()=> redis.unsubscribe('test'), 10000);
})();
