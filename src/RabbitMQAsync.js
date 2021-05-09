import amqp from 'amqplib';
import { timeout } from '@azteam/util';


class RabbitMQAsync {
    constructor(config) {
        this.connected = false;
        this.config = config;
        this.connect();
    }

    async waitConnection(n = 10) {
        let i = 0;
        while (!this.connected) {
            ++i;
            if (i >= n) {
                return false;
            }
            await timeout(1000);
        }
        return true;
    }

    async connect() {
        this._alert('connecting', 'MQ connecting...');
        const opt = { credentials: amqp.credentials.plain(this.config.username, this.config.password) };
        try {
            this.client = await amqp.connect(`amqp://${this.config.host}`, opt);
            this.connected = true;
            this._alert('connect', 'MQ connected');

            this.client.on('error', (err) => {
                this.connected = false;
                this._alert('error', 'MQ Error' + err);
                this.reconnect();
            });

            this.client.on('close', () => {
                this.connected = false;
                this._alert('close', 'MQ closed');
                this.reconnect();
            });
        } catch (err) {
            this.connected = false;
            this._alert('error', 'MQ connect' + err);
            this.reconnect();
        }
    }

    async reconnect() {
        this._alert('reconnect', 'MQ try reconnect...');
        await timeout(5000);
        this.connect();
    }

    close() {
        this.connected = false;
        this.client.close();
    }

    async send(queueName, msg = {}) {
        if (this.connected) {
            let channel = null;
            try {
                channel = await this.client.createChannel();
                await channel.assertQueue(queueName, {
                    durable: true,
                });
                await channel.sendToQueue(queueName, Buffer.from(JSON.stringify(msg)), {
                    persistent: true
                });
                return true;

            } catch (err) {
                await timeout(5000);
                return this.send(queueName, msg);
            } finally {
                try { channel && channel.close(); } catch (err) {};
            }
        } else {
            await timeout(5000);
            return this.send(queueName, msg);
        }
        return false;
    }

    async receiving(queueName, cb, callbackError = null) {
        if (this.connected) {
            let channel = null;
            try {
                channel = await this.client.createChannel();

                await channel.assertQueue(queueName, {
                    durable: true
                });
                await channel.prefetch(1);
                channel.consume(queueName, async function(msg) {
                    try {
                        const data = JSON.parse(msg.content.toString());
                        await cb(data);
                        await channel.ack(msg);
                    } catch (err) {
                        try {
                            await channel.nack(msg);
                        } catch (err) {
                            try { channel && channel.close(); } catch (err) {};
                            await timeout(5000);
                            callbackError && callbackError(queueName, err);
                            return this.receiving(queueName, cb, callbackError);
                        } finally {
                            callbackError && callbackError(queueName, err);
                        }
                    }
                })
            } catch (err) {
                try { channel && channel.close(); } catch (err) {};
                await timeout(5000);
                callbackError && callbackError(queueName, err);
                return this.receiving(queueName, cb, callbackError);
            }
        } else {
            await timeout(5000);
            return this.receiving(queueName, cb, callbackError);
        }
    }

    setAlertCallback(callback) {
        this.alertCallback = callback;
    }

    _alert(status, msg) {
        if (typeof this.alertCallback === 'function') {
            this.alertCallback(status, msg);
        } else {
            console.log(status, msg);
        }
    }
}

export default RabbitMQAsync;