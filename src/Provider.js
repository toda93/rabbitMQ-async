import RabbitMQAsync from './RabbitMQAsync';

class Provider {
    constructor(configs) {
        this.configs = configs;
        this.connections = {};
    }
    async getConnection(name) {
        if (!this.connections[name]) {
            this.connections[name] = new RabbitMQAsync(this.configs[name]);
        }
        return this.connections[name];
    }

}

export default Provider;