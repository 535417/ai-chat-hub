/**
 * Vercel 会识别项目根目录的 server.js，并把默认导出的 Express 应用当作单个 Serverless 函数。
 * 本地开发仍使用：npm start → node server/index.js
 */
export { default } from './server/index.js';
