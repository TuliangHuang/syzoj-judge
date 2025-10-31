import Bluebird = require('bluebird');
import redis = require('redis');
import Redlock = require('redlock');

import { globalConfig as Cfg } from './config';
import { codeFingerprint } from '../utils';
import { redisMetadataSuffix } from '../interfaces';

Bluebird.promisifyAll(redis.RedisClient.prototype);
Bluebird.promisifyAll(redis.Multi.prototype);

const redisClient = redis.createClient(Cfg.redis, { detect_buffers: true }) as any;
// We use one client for now, cluster support to be added later.
const redlock = new Redlock([redisClient], {
    retryCount: 40,
    retryDelay: 250
});

export async function checkBinaryExistance(name: string): Promise<Boolean> {
    return !!(await redisClient.existsAsync(name + redisMetadataSuffix));
}

const lockTTL = 5000; // ms
const maxWaitMs = 15000; // ms, after this we attempt a stale lock reset

async function forceResetLockIfStale(resource: string, heartbeatKey: string): Promise<boolean> {
    try {
        const ttl: number = await redisClient.pttlAsync(resource);
        if (ttl === -2) return true; // no lock exists
        const rawTs: string | null = await redisClient.getAsync(heartbeatKey);
        const hbTs = rawTs ? parseInt(rawTs, 10) : 0;
        const hbAge = hbTs ? Date.now() - hbTs : Number.POSITIVE_INFINITY;
        if (ttl === -1 || hbAge > lockTTL * 2) {
            await redisClient.delAsync(resource);
            await redisClient.delAsync(heartbeatKey);
            return true;
        }
    } catch (_) {
        // ignore and treat as not reset
    }
    return false;
}

export async function getCompileLock(name: string): Promise<() => Promise<void>> {
    const resource = `compile-${name}`;
    const heartbeatKey = `${resource}:hb`;

    const start = Date.now();
    while (true) {
        try {
            const lock = await redlock.lock(resource, lockTTL);
            // set initial heartbeat
            await redisClient.setAsync(heartbeatKey, String(Date.now()));
            await redisClient.pexpireAsync(heartbeatKey, lockTTL * 2);

            const token = setInterval(async () => {
                try {
                    await lock.extend(lockTTL);
                    await redisClient.setAsync(heartbeatKey, String(Date.now()));
                    await redisClient.pexpireAsync(heartbeatKey, lockTTL * 2);
                } catch (_) {
                    // ignore extension failures; next tick will retry or the lock will eventually expire
                }
            }, Math.floor(lockTTL * 0.7));

            return async () => {
                clearInterval(token);
                try { await lock.unlock(); } catch (_) { /* ignore */ }
                try { await redisClient.delAsync(heartbeatKey); } catch (_) { /* ignore */ }
            };
        } catch (e) {
            if (Date.now() - start >= maxWaitMs) {
                const reset = await forceResetLockIfStale(resource, heartbeatKey);
                if (!reset) break; // give up if we couldn't reset safely
                // after reset, loop to retry acquisition immediately
            }
            // small delay before next attempt
            await Bluebird.delay(200);
        }
    }
    // fallback: no lock acquired; return no-op unlock to avoid caller crashing
    return async () => {};
}