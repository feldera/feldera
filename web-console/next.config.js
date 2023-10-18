
const UnpluginIcons = require("unplugin-icons/webpack")
const { FileSystemIconLoader } = require("unplugin-icons/loaders")

/** @type {import("next").NextConfig} */
module.exports = {
  output: 'export', // https://nextjs.org/docs/app/building-your-application/deploying/static-exports
  distDir: process.env.BUILD_DIR || 'out',
  trailingSlash: true,
  reactStrictMode: true,
  compiler: {},
  images: { unoptimized: true },
  experimental: {
    esmExternals: false,
  },
  webpack(config) {
    config.plugins.push(
      require('unplugin-icons/webpack')({
        compiler: 'jsx',
        jsx: 'react',
      }),
    )
    config.plugins.push(
      UnpluginIcons({
        compiler: 'jsx', jsx: 'react',
        customCollections: {
          'vendors': FileSystemIconLoader(
            "./public/icons/vendors",
          ),
          'generic': FileSystemIconLoader(
            "./public/icons/generic",
          ),
        }
      })
    )
    return config
  },
}
