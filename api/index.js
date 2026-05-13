/**
 * Vercel Serverless 入口：通过 vercel.json 的 builds + routes 显式挂载，
 * 避免「零配置」未识别导致 output 为空、全站 NOT_FOUND。
 */
import app from '../server/index.js';

export default app;
