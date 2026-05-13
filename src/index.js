/**
 * Vercel 官方 Express 示例使用 `src/index.*` 作为 Serverless 入口。
 * 根目录 `server.js` 在部分项目/版本下不会被识别，会导致除 public 外全部 404。
 */
export { default } from '../server/index.js';
