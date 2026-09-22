const { SnerdQueue } = require('./dist/index.js');

async function run() {
    console.log('Booting Snerd Queue with Dashboard...');
    const queue = new SnerdQueue({
        storagePath: './.snerdata_test_dashboard',
        shards: 2,
        maxLocalShards: 2
    });

    queue.startDashboard(8080);
    console.log('Dashboard should be available at http://localhost:8080');

    queue.registerHandler('test.dummy', async (data) => {
        await new Promise(r => setTimeout(r, 500));
    });

    let count = 0;
    setInterval(async () => {
        count++;
        try {
            await queue.enqueue({
                id: `dummy-task-${Date.now()}`,
                type: 'test.dummy',
                data: { msg: `Hello ${count}` }
            });
        } catch (e) {
            console.error('Failed to enqueue:', e);
        }
    }, 5000);

    process.on('SIGINT', async () => {
        console.log('Shutting down gracefully...');
        await queue.shutdown();
        process.exit(0);
    });
}

run().catch(console.error);
