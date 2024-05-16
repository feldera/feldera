
/** @type {import("next").NextConfig} */
module.exports = {
  output: 'export', // https://nextjs.org/docs/app/building-your-application/deploying/static-exports
  distDir: process.env.BUILD_DIR || '.next',
  trailingSlash: true,
  reactStrictMode: true,
  compiler: {
  },
  images: { unoptimized: true },
  experimental: {
    esmExternals: false,
  },
  webpack(config, options) {
    config.module.rules.push({
      test: /\.sql/,
      use: [
        {
          loader: 'text',
        },
      ],
    })
    config.module.rules.push({
      test: /\.svg$/,
      issuer: { and: [/\.(js|ts|md)x?$/] },
      include: [options.dir],
      use: [
        options.defaultLoaders.babel,
        {
          loader: '@svgr/webpack',
          options: { babel: false }
        }
      ],
    })
    return config
  },
}
