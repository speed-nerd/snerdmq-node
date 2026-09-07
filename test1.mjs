import { SnerdQueue } from 'snerdmq';
const q = new SnerdQueue({ binaryPath: '../snerdmq/target/debug/snerdmq', storagePath: '/var/folders/ng/7mfhzw454_s1t4ssm3vr_92w0000gn/T/tmp.1Y2vOaon6B/s1' });
console.log('OK');
await q.shutdown();
