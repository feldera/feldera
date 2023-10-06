// eslint-disable-next-line @typescript-eslint/no-var-requires
const path = require('path')

/** @type {import("next").NextConfig} */
module.exports = {
  output: 'export', // https://nextjs.org/docs/app/building-your-application/deploying/static-exports
  distDir: process.env.BUILD_DIR || 'out',
  trailingSlash: true,
  reactStrictMode: true,
  compiler: {},
  images: { unoptimized: true },
  experimental: {
    esmExternals: false
  }
}
